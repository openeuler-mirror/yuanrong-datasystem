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
 * Description: Data system Object Client implementation.
 */

#include "datasystem/client/object_cache/object_client_impl.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <shared_mutex>
#include <string>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <tbb/concurrent_hash_map.h>
#include <nlohmann/json.hpp>

#include "datasystem/client/client_flags_monitor.h"
#include "datasystem/client/mmap/immap_table_entry.h"
#include "datasystem/client/object_cache/client_worker_api/iclient_worker_api.h"
#include "datasystem/common/device/device_manager_factory.h"
#include "datasystem/common/device/device_helper.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/metrics/metrics.h"
#include "datasystem/common/object_cache/buffer_composer.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/log/spdlog/provider.h"
#include "datasystem/common/parallel/parallel_for.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#ifdef USE_URMA
#include "datasystem/common/rdma/urma_manager.h"
#endif
#include "datasystem/common/rdma/npu/remote_h2d_manager.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/client/hetero_cache/device_buffer.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/meta_transport.pb.h"
#include "datasystem/protos/object_posix.stub.rpc.pb.h"
#include "datasystem/protos/utils.pb.h"
#include "datasystem/utils/optional.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"
#include "datasystem/utils/string_view.h"
#include "datasystem/object/buffer.h"

DS_DECLARE_bool(log_monitor);

const size_t MSET_MAX_KEY_COUNT = 8;
static constexpr size_t OBJ_META_MAX_SIZE_LIMIT = 64;
static constexpr size_t QUERY_SIZE_OBJECT_LIMIT = 10000;
const std::string K_SEPARATOR = "$";
const std::string CLIENT_PARALLEL_THREAD_MIN_NUM_ENV = "CLIENT_PARALLEL_THREAD_MIN_NUM";
const std::string CLIENT_PARALLEL_THREAD_MAX_NUM_ENV = "CLIENT_PARALLEL_THREAD_MAX_NUM";
const std::string CLIENT_MEMORY_COPY_THREAD_NUM_ENV = "CLIENT_MEMORY_COPY_THREAD_NUM";
const std::string CLIENT_MEMORY_COPY_THREAD_NUM_PER_KEY_ENV = "CLIENT_MEMORY_COPY_THREAD_NUM_PER_KEY";
const std::string CLIENT_MEMCOPY_PARALLEL_THRESHOLD_ENV = "CLIENT_MEMCOPY_PARALLEL_THRESHOLD";
static constexpr int SHM_REF_RECONCILE_INTERVAL_MS = 5 * 1000;
constexpr uint64_t CLIENT_LOCAL_OR_RPC_SLOW_US = 1000;
constexpr double US_PER_MS = 1000.0;
thread_local bool g_isThroughUb = false;

namespace datasystem {
namespace {
constexpr size_t MIN_SHUFFLE_CANDIDATE_COUNT = 2;

void ShuffleWorkerCandidates(std::vector<HostPort> &candidates)
{
    if (candidates.size() < MIN_SHUFFLE_CANDIDATE_COUNT) {
        return;
    }
    std::mt19937 generator(static_cast<uint32_t>(RandomData::GetRandomSeed()));
    std::shuffle(candidates.begin(), candidates.end(), generator);
}
}  // namespace

inline void ReadFromEnv(std::string &param, std::string env)
{
    if (param.empty()) {
        param = (std::getenv(env.c_str()) == nullptr) ? "" : std::getenv(env.c_str());
    }
}

inline void ReadFromEnv(SensitiveValue &param, std::string env)
{
    if (param.Empty()) {
        param = (std::getenv(env.c_str()) == nullptr) ? "" : std::getenv(env.c_str());
    }
}

inline void ReadParamFromEnv(ConnectOptions &connectOptions)
{
    ReadFromEnv(connectOptions.clientPublicKey, "DATASYSTEM_CLIENT_PUBLIC_KEY");
    ReadFromEnv(connectOptions.clientPrivateKey, "DATASYSTEM_CLIENT_PRIVATE_KEY");
    ReadFromEnv(connectOptions.serverPublicKey, "DATASYSTEM_SERVER_PUBLIC_KEY");
    ReadFromEnv(connectOptions.accessKey, "DATASYSTEM_ACCESS_KEY");
    ReadFromEnv(connectOptions.secretKey, "DATASYSTEM_SECRET_KEY");
    ReadFromEnv(connectOptions.tenantId, "DATASYSTEM_TENANT_ID");
}

inline void ReadOptFromEnv(ConnectOptions &connectOptions)
{
    ReadFromEnv(connectOptions.host, "DATASYSTEM_HOST");
    if (connectOptions.port == 0 && connectOptions.serviceDiscovery == nullptr) {
        int32_t envPort;
        if (std::getenv("DATASYSTEM_PORT") != nullptr && Uri::StrToInt32(std::getenv("DATASYSTEM_PORT"), envPort)) {
            connectOptions.port = envPort;
        } else {
            LOG(ERROR) << "Invalid worker port in connectOptions!";
            connectOptions.port = -1;
        }
    }
    int32_t envConnectTimeoutMs;
    connectOptions.connectTimeoutMs =
        (std::getenv("DATASYSTEM_CONNECT_TIME_MS") != nullptr
         && Uri::StrToInt32(std::getenv("DATASYSTEM_CONNECT_TIME_MS"), envConnectTimeoutMs))
            ? envConnectTimeoutMs
            : connectOptions.connectTimeoutMs;
    ReadParamFromEnv(connectOptions);
}

void AccessRecord(AccessRecorder &accessPoint, const Status &rc, const std::vector<DeviceBlobList> &BlobLists,
                  const std::vector<std::string> &keys)
{
    uint64_t totalSize = 0;
    const uint64_t max_val = std::numeric_limits<uint64_t>::max();
    for (const auto &deviceBlobList : BlobLists) {
        for (const auto &blob : deviceBlobList.blobs) {
            if (blob.size > 0 && max_val - totalSize < blob.size) {
                // maybe overflow？
                totalSize = max_val;
            } else {
                totalSize += blob.size;
            }
        }
    }
    RequestParam reqParam =
        RequestParam{ .objectKey =
                          FormatString("%s+count:%s", keys.empty() ? "" : keys[0].substr(0, LOG_OBJECT_KEY_SIZE_LIMIT),
                                       keys.size()) };
    accessPoint.Record(rc.GetCode(), std::to_string(totalSize), reqParam, rc.GetMsg());
};

struct AsyncMGetH2DState {
    std::promise<AsyncResult> promise;
    std::future<Status> rpcFuture;
    std::vector<std::string> objectKeys;
    std::vector<DeviceBlobList> devBlobList;
    // Hold buffers until copy thread completes and release is done.
    std::vector<Optional<Buffer>> bufferList;
    std::vector<Buffer *> existBufferList;
    std::vector<std::string> failedKeys;

    AsyncMGetH2DState(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &blobs)
        : objectKeys(keys), devBlobList(blobs)
    {
    }
};

struct AsyncMSetD2HState {
    std::vector<std::string> objectKeys;
    std::vector<DeviceBlobList> devBlobList;
    SetParam setParam;

    AsyncMSetD2HState(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &blobs,
                      const SetParam &param)
        : objectKeys(keys), devBlobList(blobs), setParam(param)
    {
    }
};

namespace object_cache {
ObjectClientImpl::ObjectClientImpl(const ConnectOptions &connectOptions1)
{
    (void)Provider::Instance();
    intern::StringPool::InitAll(false);
    clientStateManager_ = std::make_unique<ClientStateManager>();
    ConnectOptions connectOptions = connectOptions1;
    ReadOptFromEnv(connectOptions);
    ipAddress_ = HostPort(connectOptions.host, connectOptions.port);
    connectTimeoutMs_ = connectOptions.connectTimeoutMs;
    requestTimeoutMs_ = connectOptions.requestTimeoutMs != 0 ? connectOptions.requestTimeoutMs : connectTimeoutMs_;
    token_ = connectOptions.token;
    tenantId_ = connectOptions.tenantId;
    signature_ = std::make_unique<Signature>(connectOptions.accessKey, connectOptions.secretKey);
    enableExclusiveConnection_ = connectOptions.enableExclusiveConnection;
    enableCrossNodeConnection_ = connectOptions.enableCrossNodeConnection;
    (void)authKeys_.SetClientPublicKey(connectOptions.clientPublicKey);
    (void)authKeys_.SetClientPrivateKey(connectOptions.clientPrivateKey);
    LOG_IF_ERROR(authKeys_.SetServerKey(WORKER_SERVER_NAME, connectOptions.serverPublicKey),
                 "RpcAuthKeys SetServerKey failed");
    enableRemoteH2D_ = connectOptions.enableRemoteH2D;
    serviceDiscovery_ = connectOptions.serviceDiscovery;
    fastTransportMemSize_ = connectOptions.fastTransportMemSize;
    deviceId_ = connectOptions.deviceId;
}

ObjectClientImpl::~ObjectClientImpl()
{
    auto shutdownFunc = std::bind(&ObjectClientImpl::ShutDown, this, true, true);
    clientStateManager_->ProcessDestruct(shutdownFunc);
}

Status ObjectClientImpl::ShutDown(bool &needRollbackState, bool isDestruct)
{
    ShutdownMetricsThread(!isDestruct);
    ShutdownPerfThread();
    ShutdownShmRefReconcileThread();
    INJECT_POINT("ObjClient.ShutDown");
    // Step0: Check client's status to determine whether it meets the conditions for executing shutdown.
    Status rc = clientStateManager_->ProcessShutdown(needRollbackState, isDestruct);
    if (!needRollbackState) {
        return rc;
    }
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();

    asyncSetRPCPool_ = nullptr;
    asyncGetRPCPool_ = nullptr;
    asyncGetCopyPool_ = nullptr;
    asyncDevDeletePool_ = nullptr;
    // Protect concurrent read and write access to asyncReleasePool_ variable and don't hold the lock when release the
    // thread pool.
    auto asyncReleasePool = asyncReleasePool_;
    {
        std::lock_guard<std::shared_timed_mutex> lck(shutdownMux_);
        asyncReleasePool_ = nullptr;
    }
    asyncReleasePool = nullptr;

    if (devOcImpl_ != nullptr) {
        devOcImpl_->SetThreadInterruptFlag2True();
    }

    // Step0: notify wait post.
    switchPost_.Set();
    // Step1: Shutdown heartbeat.
    for (size_t i = 0; i < listenWorker_.size(); i++) {
        if (listenWorker_[i] != nullptr) {
            listenWorker_[i]->StopListenWorker(true);
        }
    }
    // Step2: keep the local worker disconnect under the shutdown lock because it shares
    // the same shutdown-synchronized shm ref cleanup path. Other worker disconnects can
    // be deferred until after the lock is released.
    std::vector<std::shared_ptr<IClientWorkerApi>> deferredDisconnectApis;
    {
        std::lock_guard<std::shared_timed_mutex> lck(shutdownMux_);
        deferredDisconnectApis.reserve(workerApi_.size());
        for (size_t i = 0; i < workerApi_.size(); i++) {
            if (workerApi_[i] != nullptr && CheckConnection(static_cast<WorkerNode>(i)).IsOk()) {
                if (i == LOCAL_WORKER) {
                    auto curRc = workerApi_[i]->Disconnect(isDestruct);
                    if (curRc.IsError()) {
                        rc = std::move(curRc);
                    }
                    continue;
                }
                deferredDisconnectApis.push_back(workerApi_[i]);
            }
        }
    }
    for (const auto &api : deferredDisconnectApis) {
        auto curRc = api->Disconnect(isDestruct);
        if (curRc.IsError()) {
            rc = std::move(curRc);
        }
    }

    // The destructor of devOcImpl_ should occur after the client disconnect request so that the device asynchronous
    // threads can exit quickly.
    devOcImpl_.reset();
    if (worker_ && embeddedClientWorkerApi_) {
        embeddedClientWorkerApi_->WorkerDestroy(worker_);
        worker_ = nullptr;
    }
    return rc;
}

Status ObjectClientImpl::ParseEmbeddedConfig(const EmbeddedConfig &config)
{
    const auto &args = config.GetArgs();
    if (args.find("system_access_key") != args.end() && args.find("system_secret_key") != args.end()) {
        signature_->SetClientAkSk(args.at("system_access_key"), args.at("system_secret_key"));
    }
    if (args.find("connectTimeoutMs") != args.end()) {
        int result = 0;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Uri::StrToInt(args.at("connectTimeoutMs").c_str(), result),
                                             K_RUNTIME_ERROR, "connectTimeoutMs to int failed");
        connectTimeoutMs_ = result;
    }
    return Status::OK();
}

Status ObjectClientImpl::InitEmbedded(const EmbeddedConfig &config, bool &needRollbackState)
{
    auto rc = clientStateManager_->ProcessInit(needRollbackState);
    if (!needRollbackState) {
        return rc;
    }
    RETURN_IF_NOT_OK(ParseEmbeddedConfig(config));
    embeddedClientWorkerApi_ = std::make_shared<datasystem::client::EmbeddedClientWorkerApi>();
    RETURN_IF_NOT_OK(embeddedClientWorkerApi_->LoadPlugin());
    worker_ = embeddedClientWorkerApi_->CreateWorker();
    CHECK_FAIL_RETURN_STATUS(worker_ != nullptr, K_RUNTIME_ERROR, "create worker failed");
    RETURN_IF_NOT_OK(embeddedClientWorkerApi_->InitEmbeddedWorker(config, worker_));
    RETURN_IF_NOT_OK(ipAddress_.ParseString(config.GetArgs().at("worker_address")));
    FlagsMonitor::GetInstance()->Start();
    LOG(INFO) << "Start to init embedded client";
    RETURN_IF_NOT_OK(InitClientWorkerConnect(false, true));
    return Status::OK();
}

void ObjectClientImpl::ConstructTreadPool()
{
    const size_t threadCount = 8;
    asyncSetRPCPool_ = std::make_shared<ThreadPool>(0, threadCount, "async_set");
    asyncGetCopyPool_ = std::make_shared<ThreadPool>(0, threadCount, "async_get_copy");
    asyncGetRPCPool_ = std::make_shared<ThreadPool>(0, threadCount, "async_get_rpc");
    asyncSwitchWorkerPool_ = std::make_shared<ThreadPool>(0, 1, "switch");
    asyncDevDeletePool_ = std::make_shared<ThreadPool>(0, threadCount);
    asyncReleasePool_ = std::make_shared<ThreadPool>(0, 1, "async_release_buffer");
}

Status ObjectClientImpl::InitClientWorkerConnect(bool enableHeartbeat, bool initWithWorker)
{
    CHECK_FAIL_RETURN_STATUS(connectTimeoutMs_ >= 0, K_INVALID, "The connection timeout must be a positive integer.");
    RETURN_IF_NOT_OK(InitClientWorkerConnectAt(LOCAL_WORKER, ipAddress_, enableHeartbeat, initWithWorker));
    return InitClientRuntimeAt(LOCAL_WORKER, initWithWorker, true);
}

Status ObjectClientImpl::InitClientWorkerConnectAt(WorkerNode node, const HostPort &address, bool enableHeartbeat,
                                                   bool initWithWorker)
{
    HeartbeatType heartbeatType = enableHeartbeat ? HeartbeatType::RPC_HEARTBEAT : HeartbeatType::NO_HEARTBEAT;
    workerApi_.resize(STANDBY2_WORKER + 1);
    if (!initWithWorker) {
        workerApi_[node] =
            std::make_shared<ClientWorkerRemoteApi>(address, cred_, heartbeatType, token_, signature_.get(), tenantId_,
                                                    enableCrossNodeConnection_, enableExclusiveConnection_, deviceId_);
    } else {
        workerApi_[node] = std::make_shared<ClientWorkerLocalApi>(address, embeddedClientWorkerApi_, worker_,
                                                                  heartbeatType, signature_.get(), false, deviceId_);
    }
    workerApi_[node]->isUseStandbyWorker_ = node != LOCAL_WORKER;
    RETURN_IF_NOT_OK(workerApi_[node]->Init(requestTimeoutMs_, connectTimeoutMs_, fastTransportMemSize_));
    return Status::OK();
}

Status ObjectClientImpl::InitClientRuntimeAt(WorkerNode node, bool initWithWorker, bool isLocalWorker)
{
    auto &workerApi = workerApi_[node];
    mmapManager_ = std::make_unique<client::MmapManager>(workerApi, initWithWorker);
    ConstructTreadPool();

    RETURN_IF_NOT_OK(workerApi->PrepairForDecreaseShmRef(std::bind(
        &client::MmapManager::LookupUnitsAndMmapFd, mmapManager_.get(), std::placeholders::_1, std::placeholders::_2)));
    clientEnableP2Ptransfer_ = workerApi->workerEnableP2Ptransfer_;
    RETURN_IF_NOT_OK(InitListenWorkerAt(node, isLocalWorker));
    RETURN_IF_NOT_OK(workerApi->TryFastTransportAfterHeartbeat());
    devOcImpl_ = std::make_unique<ClientDeviceObjectManager>(this);
    RETURN_IF_NOT_OK(devOcImpl_->Init());
    memoryRefCount_.SetSupportMultiShmRefCount(workerApi->workerSupportMultiShmRefCount_);
    StartShmRefReconcileThread();
    StartPerfThread();
    StartMetricsThread();
    InitParallelFor();
    return Status::OK();
}

Status ObjectClientImpl::InitListenWorker()
{
    return InitListenWorkerAt(LOCAL_WORKER, true);
}

Status ObjectClientImpl::InitListenWorkerAt(WorkerNode node, bool isLocalWorker)
{
    auto heartbeatType = workerApi_[node]->heartbeatType_;
    listenWorker_.resize(STANDBY2_WORKER + 1);
    listenWorker_[node] =
        std::make_shared<client::ListenWorker>(workerApi_[node], heartbeatType, node, asyncSwitchWorkerPool_.get());
    if (isLocalWorker) {
        listenWorker_[node]->AddCallBackFunc(this, [this] { ProcessWorkerLost(); });
        listenWorker_[node]->SetWorkerTimeoutHandle([this] { ProcessWorkerTimeout(); });
        listenWorker_[node]->SetReleaseFdCallBack(
            [this](const std::vector<int64_t> &fds) { mmapManager_->ClearExpiredFds(fds); });
    } else {
        listenWorker_[node]->AddCallBackFunc(this, [this, node]() { ProcessStandbyWorkerLost(node); });
        if (serviceDiscovery_ != nullptr && serviceDiscovery_->HasHostAffinity()) {
            listenWorker_[node]->SetRecoverLocalWorkerHandle([this]() { return RecoverPreferredLocalWorker(); });
        }
    }
    if (enableCrossNodeConnection_) {
        listenWorker_[node]->SetSwitchWorkerHandle([this](uint32_t index, client::SwitchTriggerReason reason) {
            return SwitchWorkerNode(static_cast<WorkerNode>(index), reason);
        });
    }
    listenWorker_[node]->SetIsLocalWorker(isLocalWorker);
    RETURN_IF_NOT_OK(listenWorker_[node]->StartListenWorker());
    return Status::OK();
}

Status ObjectClientImpl::InitPreferredRemoteFallback(const HostPort &remoteAddress, bool enableHeartbeat)
{
    CHECK_FAIL_RETURN_STATUS(connectTimeoutMs_ >= 0, K_INVALID, "The connection timeout must be a positive integer.");
    RETURN_IF_NOT_OK(InitClientWorkerConnectAt(STANDBY1_WORKER, remoteAddress, enableHeartbeat, false));
    currentNode_ = STANDBY1_WORKER;
    RETURN_IF_NOT_OK(InitClientRuntimeAt(STANDBY1_WORKER, false, false));
    LOG(INFO) << "[Switch] Preferred same-node local worker is absent, use remote fallback "
              << remoteAddress.ToString();
    return Status::OK();
}

Status ObjectClientImpl::Init(bool &needRollbackState, bool enableHeartbeat)
{
    Logging::GetInstance()->Start(CLIENT_LOG_FILENAME, true);
    FlagsMonitor::GetInstance()->Start();

    auto rc = clientStateManager_->ProcessInit(needRollbackState);
    if (!needRollbackState) {
        return rc;
    }

    if (serviceDiscovery_ != nullptr) {
        std::string workerIp;
        int workerPort;
        bool isSameNode = false;
        RETURN_IF_NOT_OK(serviceDiscovery_->SelectWorker(workerIp, workerPort, &isSameNode));
        HostPort selectedAddress(workerIp, workerPort);
        if (!isSameNode && serviceDiscovery_->HasHostAffinity()) {
            std::string hostPortStr = selectedAddress.ToString();
            CHECK_FAIL_RETURN_STATUS(Validator::ValidateHostPortString("HostPort", hostPortStr), K_INVALID,
                                     FormatString("Invalid IP address/port. Host %s, port: %d", selectedAddress.Host(),
                                                  selectedAddress.Port()));

            LOG(INFO) << "Start to init preferred remote fallback worker client at address: " << hostPortStr;
            RETURN_IF_NOT_OK(RpcAuthKeyManager::CreateClientCredentials(authKeys_, WORKER_SERVER_NAME, cred_));
            RETURN_IF_NOT_OK(InitPreferredRemoteFallback(selectedAddress, enableHeartbeat));
            return Status::OK();
        }
        ipAddress_ = selectedAddress;
    }
    std::string hostPortStr = ipAddress_.ToString();
    if (hostPortStr.empty()) {
        return Status(K_INVALID, "ConnectOptions was not configured with a host and port or serviceDiscovery.");
    }

    CHECK_FAIL_RETURN_STATUS(
        Validator::ValidateHostPortString("HostPort", hostPortStr), K_INVALID,
        FormatString("Invalid IP address/port. Host %s, port: %d", ipAddress_.Host(), ipAddress_.Port()));

    LOG(INFO) << "Start to init worker client at address: " << hostPortStr;
    RETURN_IF_NOT_OK(RpcAuthKeyManager::CreateClientCredentials(authKeys_, WORKER_SERVER_NAME, cred_));
    RETURN_IF_NOT_OK(InitClientWorkerConnect(enableHeartbeat, false));
    return Status::OK();
}

void ObjectClientImpl::InitParallelFor()
{
    static const int defaultThreadNum = 4;
    auto getEnvInt = [](const std::string &envName, int defaultValue) -> int {
        const char *val = std::getenv(envName.c_str());
        int result = defaultValue;
        if (val && !Uri::StrToInt(val, result)) {
            result = defaultValue;
        }
        return result;
    };

    int threadNum = -1;
    threadNum = getEnvInt(CLIENT_MEMORY_COPY_THREAD_NUM_PER_KEY_ENV, threadNum);
    if (threadNum == -1) {
        memoryCopyThreadPool_ = std::make_shared<ThreadPool>(0, GetRecommendedMemoryCopyThreadsNum());
    } else if (threadNum > 0) {
        memoryCopyThreadPool_ = std::make_shared<ThreadPool>(threadNum);
    }
    memcpyParallelThreshold_ = getEnvInt(CLIENT_MEMCOPY_PARALLEL_THRESHOLD_ENV, MEMCOPY_PARALLEL_THRESHOLD);

    parallismNum_ = getEnvInt(CLIENT_MEMORY_COPY_THREAD_NUM_ENV, defaultThreadNum);
    int minThreadNum = getEnvInt(CLIENT_PARALLEL_THREAD_MIN_NUM_ENV, defaultThreadNum);
    minThreadNum = minThreadNum < parallismNum_ ? parallismNum_ : minThreadNum;
    int maxThreadNum = getEnvInt(CLIENT_PARALLEL_THREAD_MAX_NUM_ENV, minThreadNum);
    LOG(INFO) << FormatString("Init parallel for with parallismNum: %d, minThreadNum: %d, maxThreadNum: %d",
                              parallismNum_, minThreadNum, maxThreadNum);
    if (minThreadNum == 0) {
        return;
    }
    datasystem::Parallel::InitParallelThreadPool(minThreadNum, maxThreadNum);
}

void ObjectClientImpl::ProcessWorkerLost()
{
    if (clientStateManager_->GetState() & (uint16_t)ClientState::EXITED) {
        return;
    }
    ProcessWorkerTimeout();
    auto &workerApi = workerApi_[LOCAL_WORKER];
    LOG(INFO) << "[Reconnect] Clear meta and try reconnect to " << ipAddress_.ToString();
    std::vector<std::string> ids;
    {
        std::lock_guard<std::shared_timed_mutex> l(globalRefMutex_);
        ids.reserve(globalRefCount_.size());
        for (const auto &entry : globalRefCount_) {
            ids.emplace_back(entry.first);
        }
    }
    Status s = workerApi->ReconnectWorker(ids);
    if (s.IsError()) {
        LOG(ERROR) << "[Reconnect] Reconnect local worker failed, error message: " << s.ToString();
        return;
    }
    memoryRefCount_.SetSupportMultiShmRefCount(workerApi->workerSupportMultiShmRefCount_);
    auto rc = workerApi->PrepairForDecreaseShmRef(std::bind(
        &client::MmapManager::LookupUnitsAndMmapFd, mmapManager_.get(), std::placeholders::_1, std::placeholders::_2));
    if (rc.IsError()) {
        LOG(ERROR) << "[Reconnect] Failed to prepair for DecreaseShmRef:" << rc.ToString();
        return;
    }
    listenWorker_[LOCAL_WORKER]->SetWorkerAvailable(true);
    {
        std::lock_guard<std::mutex> lock(switchNodeMutex_);
        if (currentNode_ == LOCAL_WORKER) {
            MarkWorkerAvailableLocked();
        }
    }
    LOG(INFO) << "[Reconnect] Reconnect to local worker success.";
    INJECT_POINT("ObjectClientImpl.ProcessWorkerLost", []() {});
}

void ObjectClientImpl::ProcessWorkerTimeout()
{
    if (clientStateManager_->GetState() & (uint16_t)ClientState::EXITED) {
        return;
    }
    auto &workerApi = workerApi_[LOCAL_WORKER];
    (void)workerApi->CleanUpForDecreaseShmRefAfterWorkerLost();
    mmapManager_->CleanInvalidMmapTable();
    // Only shm object would record reference count, and they are
    // unrecoverable after timeout until worker reconnects, so clear them directly.
    memoryRefCount_.Clear();
}

void ObjectClientImpl::ProcessStandbyWorkerLost(WorkerNode node)
{
    if (clientStateManager_->GetState() & (uint16_t)ClientState::EXITED) {
        return;
    }
    if (workerApi_[node] == nullptr) {
        LOG(ERROR) << FormatString("[Reconnect] client %d is null", node);
        return;
    }
    LOG(INFO) << FormatString("[Reconnect] Client[%d] %s try to reconnect to %s", node, workerApi_[node]->clientId_,
                              workerApi_[node]->hostPort_.ToString());
    Status s = workerApi_[node]->ReconnectWorker({});
    if (s.IsError()) {
        LOG(ERROR) << FormatString("[Reconnect] client[%d] %s reconnect to worker failed: %s", node,
                                   workerApi_[node]->clientId_, s.ToString());
        return;
    }
    if (listenWorker_[node] != nullptr) {
        listenWorker_[node]->SetWorkerAvailable(true);
    }
    {
        std::lock_guard<std::mutex> lock(switchNodeMutex_);
        if (currentNode_ == node) {
            MarkWorkerAvailableLocked();
        }
    }
    LOG(INFO) << FormatString("[Reconnect] Client[%d] %s reconnect to worker %s success.", node,
                              workerApi_[node]->clientId_, workerApi_[node]->hostPort_.ToString());
}

ObjectClientImpl::WorkerNode ObjectClientImpl::GetNextWorkerNode(WorkerNode current)
{
    switch (current) {
        case LOCAL_WORKER:
        case STANDBY2_WORKER:
            return STANDBY1_WORKER;
        case STANDBY1_WORKER:
            return STANDBY2_WORKER;
        default:
            return STANDBY1_WORKER;
    }
}

void ObjectClientImpl::StopStandbyWorkerListen(WorkerNode id)
{
    if (id == LOCAL_WORKER || listenWorker_[id] == nullptr) {
        return;
    }
    listenWorker_[id]->StopListenWorker(false);
}

void ObjectClientImpl::MarkWorkerAvailableLocked()
{
    workerSwitchState_ = WorkerSwitchState::AVAILABLE;
    switchInProgress_ = false;
    ++switchGeneration_;
}

void ObjectClientImpl::MarkNoSwitchableWorkerLocked()
{
    LOG(WARNING) << "[Switch] No switchable worker available, enable fail-fast.";
    workerSwitchState_ = WorkerSwitchState::NO_SWITCHABLE_WORKER;
    switchInProgress_ = false;
    ++switchGeneration_;
}

Status ObjectClientImpl::NoSwitchableWorkerStatus() const
{
    return { K_RPC_UNAVAILABLE, "no switchable worker available" };
}

bool ObjectClientImpl::SwitchWorkerNode(WorkerNode node, client::SwitchTriggerReason reason)
{
    if (clientStateManager_->GetState() & (uint16_t)ClientState::EXITED) {
        return true;
    }
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::shared_ptr<IClientWorkerApi> nextWorkerApi;
    std::shared_ptr<client::ListenWorker> nextListenWorker;
    WorkerNode current;
    WorkerNode next = LOCAL_WORKER;
    uint64_t switchGeneration = 0;
    bool switchBackToLocal = false;
    {
        std::lock_guard<std::mutex> lock(switchNodeMutex_);
        current = currentNode_;
        if (current != node && node != LOCAL_WORKER) {
            LOG(INFO) << FormatString("[Switch] Current node is %d, not %d, just ignore...", current, node);
            return true;
        }

        if (current != node && node == LOCAL_WORKER) {
            switchBackToLocal = true;
        } else {
            if (switchInProgress_) {
                VLOG(1) << "[Switch] Worker switch is already in progress";
                return false;
            }
            workerApi = workerApi_[current];
            if (workerApi == nullptr) {
                LOG(ERROR) << "[Switch] current worker is null pointer";
                return false;
            }
            next = GetNextWorkerNode(current);
            nextWorkerApi = workerApi_[next];
            nextListenWorker = listenWorker_[next];
            workerSwitchState_ = WorkerSwitchState::SWITCHING;
            switchInProgress_ = true;
            switchGeneration = ++switchGeneration_;
        }
    }

    if (switchBackToLocal) {
        return TrySwitchBackToLocalWorker();
    }
    // If next stub still has requests to be processed, wait for next time.
    if (!ReadyToExit(next, nextWorkerApi, nextListenWorker)) {
        std::lock_guard<std::mutex> lock(switchNodeMutex_);
        if (switchInProgress_ && switchGeneration_ == switchGeneration && currentNode_ == current) {
            MarkWorkerAvailableLocked();
        }
        return false;
    }
    return SwitchToStandbyWorkerImpl(workerApi, current, next, switchGeneration, reason);
}

bool ObjectClientImpl::SwitchToStandbyWorkerImpl(const std::shared_ptr<IClientWorkerApi> &currentApi,
                                                 WorkerNode current, WorkerNode next, uint64_t switchGeneration,
                                                 client::SwitchTriggerReason reason)
{
    PerfPoint perfPoint(PerfKey::CLIENT_SWITCH_STANDBY_WORKER);
    Raii switchEndNotifier([]() { INJECT_POINT_NO_RETURN("client.switch_worker_end", []() { return true; }); });
    const bool keepCurrentWorker = reason == client::SwitchTriggerReason::VOLUNTARY_SCALE_DOWN;
    std::vector<HostPort> sameHost;
    std::vector<HostPort> others;
    GetStandbyWorkersForSwitch(currentApi, sameHost, others);
    if (sameHost.empty() && others.empty()) {
        LOG(ERROR) << "[Switch] standby worker list is empty";
        if (keepCurrentWorker) {
            RestoreWorkerAvailableIfNeeded(current, switchGeneration);
        } else {
            MarkNoSwitchableWorkerIfNeeded(current, switchGeneration);
        }
        return false;
    }

    // Same-host candidates replace the LOCAL_WORKER slot; others go into a standby slot.
    auto result = TrySwitchToCandidateList(currentApi, current, next, switchGeneration, sameHost, true);
    if (result == StandbySwitchAttemptResult::SWITCHED) {
        return true;
    }
    if (result == StandbySwitchAttemptResult::ABORT) {
        return false;
    }
    result = TrySwitchToCandidateList(currentApi, current, next, switchGeneration, others, false);
    if (result == StandbySwitchAttemptResult::SWITCHED) {
        return true;
    }
    if (result == StandbySwitchAttemptResult::ABORT) {
        return false;
    }
    if (keepCurrentWorker) {
        RestoreWorkerAvailableIfNeeded(current, switchGeneration);
    } else {
        MarkNoSwitchableWorkerIfNeeded(current, switchGeneration);
    }
    return false;
}

ObjectClientImpl::StandbySwitchAttemptResult ObjectClientImpl::TrySwitchToCandidateList(
    const std::shared_ptr<IClientWorkerApi> &currentApi, WorkerNode current, WorkerNode next, uint64_t switchGeneration,
    const std::vector<HostPort> &candidates, bool isSameHost)
{
    for (const auto &addr : candidates) {
        if (addr.Empty()) {
            if (!isSameHost) {
                LOG(INFO) << "[Switch] Current worker has not standby worker.";
            }
            continue;
        }
        LOG(INFO) << FormatString("[Switch] Switch worker to %s", addr.ToString());
        // The TrySwitchBackToLocalWorker short-circuit only works on the standby path: with service
        // discovery CommitStandbySwitch stops the old LOCAL_WORKER listener, so its CheckWorkerAvailable
        // will report unavailable. Same-host candidates must go through TrySwitchToLocalSameHost below,
        // which builds a fresh listener.
        if (!isSameHost && addr == ipAddress_) {
            if (TrySwitchBackToLocalWorker()) {
                return StandbySwitchAttemptResult::SWITCHED;
            }
            continue;
        }
        auto attemptResult = isSameHost ? TrySwitchToLocalSameHost(current, switchGeneration, addr)
                                        : TrySwitchToStandbyWorker(currentApi, current, next, switchGeneration, addr);
        if (attemptResult != StandbySwitchAttemptResult::CONTINUE) {
            return attemptResult;
        }
    }
    return StandbySwitchAttemptResult::CONTINUE;
}

void ObjectClientImpl::GetStandbyWorkersForSwitch(const std::shared_ptr<IClientWorkerApi> &currentApi,
                                                  std::vector<HostPort> &sameHost, std::vector<HostPort> &others) const
{
    sameHost.clear();
    others.clear();
    if (serviceDiscovery_ != nullptr) {
        std::vector<std::string> sdSameHost;
        std::vector<std::string> sdOthers;
        Status rc = serviceDiscovery_->GetAllWorkers(sdSameHost, sdOthers);
        if (rc.IsError()) {
            LOG(WARNING) << "[Switch] Service discovery failed, falling back to heartbeat standby list: "
                         << rc.ToString();
            others = currentApi->GetStandbyWorkers();
        } else {
            const HostPort &selfAddr = currentApi->hostPort_;
            auto append = [&selfAddr](const std::vector<std::string> &addrs, std::vector<HostPort> &out) {
                for (const auto &addr : addrs) {
                    HostPort hp;
                    if (hp.ParseString(addr).IsError() || hp == selfAddr) {
                        continue;
                    }
                    out.emplace_back(std::move(hp));
                }
            };
            append(sdSameHost, sameHost);
            append(sdOthers, others);
        }
    } else {
        others = currentApi->GetStandbyWorkers();
    }
    INJECT_POINT_NO_RETURN("client.standby_worker", [&sameHost, &others](const std::string &addr) {
        HostPort hostPort;
        hostPort.ParseString(addr);
        sameHost.clear();
        others.clear();
        others.emplace_back(hostPort);
        return true;
    });
    ShuffleWorkerCandidates(sameHost);
    ShuffleWorkerCandidates(others);
}

bool ObjectClientImpl::CommitStandbySwitch(WorkerNode current, WorkerNode next, uint64_t switchGeneration,
                                           const std::shared_ptr<IClientWorkerApi> &candidateWorkerApi,
                                           const std::shared_ptr<client::ListenWorker> &candidateListenWorker)
{
    std::lock_guard<std::mutex> lock(switchNodeMutex_);
    if (!switchInProgress_ || switchGeneration_ != switchGeneration || currentNode_ != current
        || (clientStateManager_->GetState() & (uint16_t)ClientState::EXITED)) {
        return false;
    }
    workerApi_[next] = candidateWorkerApi;
    listenWorker_[next] = candidateListenWorker;
    currentNode_ = next;
    // Stop the LOCAL_WORKER listener only when standby-side rediscovery can take over;
    // otherwise it is still the only recovery path.
    if (serviceDiscovery_ != nullptr && serviceDiscovery_->HasHostAffinity()
        && listenWorker_[LOCAL_WORKER] != nullptr) {
        listenWorker_[LOCAL_WORKER]->StopListenWorker(false);
    }
    MarkWorkerAvailableLocked();
    return true;
}

ObjectClientImpl::StandbySwitchAttemptResult ObjectClientImpl::TrySwitchToStandbyWorker(
    const std::shared_ptr<IClientWorkerApi> &currentApi, WorkerNode current, WorkerNode next, uint64_t switchGeneration,
    const HostPort &standbyWorker)
{
    HeartbeatType heartbeatType = currentApi->heartbeatType_;
    auto candidateWorkerApi = currentApi->CloneWith(standbyWorker, cred_, heartbeatType, token_, signature_.get(),
                                                    tenantId_, enableCrossNodeConnection_, enableExclusiveConnection_,
                                                    embeddedClientWorkerApi_, worker_);
    candidateWorkerApi->isUseStandbyWorker_ = true;
    Status rc = candidateWorkerApi->Init(requestTimeoutMs_, connectTimeoutMs_, fastTransportMemSize_);
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("[Switch] Worker(%s) init failed, error msg: %s", standbyWorker.ToString(),
                                   rc.ToString());
        return StandbySwitchAttemptResult::CONTINUE;
    }

    auto candidateListenWorker =
        std::make_shared<client::ListenWorker>(candidateWorkerApi, heartbeatType, next, asyncSwitchWorkerPool_.get());
    candidateListenWorker->SetSwitchWorkerHandle([this](uint32_t index, client::SwitchTriggerReason reason) {
        return SwitchWorkerNode(static_cast<WorkerNode>(index), reason);
    });
    candidateListenWorker->SetIsLocalWorker(false);
    if (serviceDiscovery_ != nullptr && serviceDiscovery_->HasHostAffinity()) {
        candidateListenWorker->SetRecoverLocalWorkerHandle([this]() { return RecoverPreferredLocalWorker(); });
    }
    candidateListenWorker->AddCallBackFunc(this, [this, next]() { ProcessStandbyWorkerLost(next); });
    rc = candidateListenWorker->StartListenWorker();
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("[Switch] Listen worker(%s) failed, with status: %s", standbyWorker.ToString(),
                                   rc.ToString());
        return StandbySwitchAttemptResult::CONTINUE;
    }

    rc = candidateWorkerApi->TryFastTransportAfterHeartbeat();
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("[Switch] Fast transport init failed for worker(%s), with status: %s",
                                     standbyWorker.ToString(), rc.ToString());
    }
    if (!WaitStandbyWorkerReady(candidateWorkerApi)) {
        LOG(ERROR) << FormatString("[Switch] client %s wait for worker %s ready failed", GetClientId(),
                                   candidateWorkerApi->hostPort_.ToString());
        candidateListenWorker->StopListenWorker(true);
        return StandbySwitchAttemptResult::CONTINUE;
    }
    if (!CommitStandbySwitch(current, next, switchGeneration, candidateWorkerApi, candidateListenWorker)) {
        candidateListenWorker->StopListenWorker(true);
        return StandbySwitchAttemptResult::ABORT;
    }
    LOG(INFO) << FormatString("[Switch] client %s wait for worker %s ready success", GetClientId(),
                              candidateWorkerApi->hostPort_.ToString());
    return StandbySwitchAttemptResult::SWITCHED;
}

ObjectClientImpl::StandbySwitchAttemptResult ObjectClientImpl::TrySwitchToLocalSameHost(WorkerNode current,
                                                                                        uint64_t switchGeneration,
                                                                                        const HostPort &localAddress)
{
    HeartbeatType heartbeatType = workerApi_[current]->heartbeatType_;
    std::shared_ptr<ClientWorkerRemoteApi> localWorkerApi;
    std::unique_ptr<client::MmapManager> localMmapManager;
    std::shared_ptr<client::ListenWorker> localListenWorker;
    if (PreparePreferredLocalWorker(localAddress, heartbeatType, localWorkerApi, localMmapManager, localListenWorker)
            .IsError()) {
        return StandbySwitchAttemptResult::CONTINUE;
    }
    // Declared outside the lock so the old listener's destructor (which joins its heartbeat
    // thread) runs after switchNodeMutex_ is released; otherwise it can deadlock against
    // ProcessWorkerLost waiting on the same mutex.
    std::shared_ptr<client::ListenWorker> oldLocalListener;
    std::unique_ptr<client::MmapManager> oldMmapManager;
    {
        std::lock_guard<std::mutex> lock(switchNodeMutex_);
        if (!switchInProgress_ || switchGeneration_ != switchGeneration || currentNode_ != current
            || (clientStateManager_->GetState() & (uint16_t)ClientState::EXITED)) {
            return StandbySwitchAttemptResult::ABORT;
        }
        ipAddress_ = localAddress;
        workerApi_[LOCAL_WORKER] = localWorkerApi;
        ReplacePreferredLocalWorkerLocked(localMmapManager, oldLocalListener, oldMmapManager);
        listenWorker_[LOCAL_WORKER] = localListenWorker;
        clientEnableP2Ptransfer_ = localWorkerApi->workerEnableP2Ptransfer_;
        memoryRefCount_.SetSupportMultiShmRefCount(localWorkerApi->workerSupportMultiShmRefCount_);
        currentNode_ = LOCAL_WORKER;
        if (current != LOCAL_WORKER && listenWorker_[current] != nullptr) {
            listenWorker_[current]->SetSwitched();
        }
        MarkWorkerAvailableLocked();
    }
    LOG(INFO) << "[Switch] LOCAL_WORKER replaced with same-host worker at " << localAddress.ToString();
    return StandbySwitchAttemptResult::SWITCHED;
}

void ObjectClientImpl::MarkNoSwitchableWorkerIfNeeded(WorkerNode current, uint64_t switchGeneration)
{
    std::lock_guard<std::mutex> lock(switchNodeMutex_);
    if (switchInProgress_ && switchGeneration_ == switchGeneration && currentNode_ == current) {
        MarkNoSwitchableWorkerLocked();
    }
}

void ObjectClientImpl::RestoreWorkerAvailableIfNeeded(WorkerNode current, uint64_t switchGeneration)
{
    std::lock_guard<std::mutex> lock(switchNodeMutex_);
    if (switchInProgress_ && switchGeneration_ == switchGeneration && currentNode_ == current) {
        MarkWorkerAvailableLocked();
    }
}

void ObjectClientImpl::ReplacePreferredLocalWorkerLocked(std::unique_ptr<client::MmapManager> &localMmapManager,
                                                         std::shared_ptr<client::ListenWorker> &oldLocalListener,
                                                         std::unique_ptr<client::MmapManager> &oldMmapManager)
{
    oldLocalListener = std::move(listenWorker_[LOCAL_WORKER]);
    mmapManager_.swap(localMmapManager);
    oldMmapManager = std::move(localMmapManager);
}

bool ObjectClientImpl::TrySwitchBackToLocalWorker()
{
    WorkerNode current;
    std::shared_ptr<IClientWorkerApi> localWorkerApi;
    std::shared_ptr<client::ListenWorker> localListenWorker;
    std::shared_ptr<client::ListenWorker> currentListenWorker;
    {
        std::lock_guard<std::mutex> lock(switchNodeMutex_);
        current = currentNode_;
        if (current == LOCAL_WORKER) {
            return false;
        }
        localWorkerApi = workerApi_[LOCAL_WORKER];
        localListenWorker = listenWorker_[LOCAL_WORKER];
        currentListenWorker = listenWorker_[current];
    }

    if (localWorkerApi == nullptr || localListenWorker == nullptr) {
        LOG(ERROR) << "[Switch] Local worker is not ready for switch back";
        return false;
    }
    auto s = localListenWorker->CheckWorkerAvailable();
    bool scaleDown = localListenWorker->IsWorkerVoluntaryScaleDown();
    bool healthy = localWorkerApi->healthy_;
    if (s.IsOk() && !scaleDown && healthy) {
        std::lock_guard<std::mutex> lock(switchNodeMutex_);
        if (currentNode_ == LOCAL_WORKER) {
            return true;
        }
        if (currentNode_ != current || (clientStateManager_->GetState() & (uint16_t)ClientState::EXITED)) {
            return false;
        }
        LOG(INFO) << "[Switch] Restore local worker success.";
        if (currentListenWorker != nullptr) {
            currentListenWorker->SetSwitched();
        }
        currentNode_ = LOCAL_WORKER;
        MarkWorkerAvailableLocked();
        return true;
    } else {
        constexpr int times = 10;
        LOG_EVERY_T(INFO, times) << FormatString(
            "[Switch] Restore local worker failed, connection status: %s, is scale down: %d, is healthy: %d",
            s.ToString(), scaleDown, healthy);
        return false;
    }
}

bool ObjectClientImpl::GetPreferredLocalWorkerToRecover(WorkerNode &oldNode, HostPort &localAddress,
                                                        HeartbeatType &heartbeatType)
{
    if (serviceDiscovery_ == nullptr || !serviceDiscovery_->HasHostAffinity()) {
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(switchNodeMutex_);
        if (currentNode_ == LOCAL_WORKER || (clientStateManager_->GetState() & (uint16_t)ClientState::EXITED)) {
            return false;
        }
        oldNode = currentNode_;
        if (workerApi_[oldNode] == nullptr) {
            return false;
        }
        heartbeatType = workerApi_[oldNode]->heartbeatType_;
    }

    std::string workerIp;
    int workerPort;
    Status rc = serviceDiscovery_->SelectSameNodeWorker(workerIp, workerPort);
    if (rc.IsError()) {
        constexpr int times = 10;
        LOG_EVERY_T(INFO, times) << "[Switch] Same-node worker is not ready yet: " << rc.ToString();
        return false;
    }
    localAddress = HostPort(workerIp, workerPort);
    return true;
}

Status ObjectClientImpl::PreparePreferredLocalWorker(const HostPort &localAddress, HeartbeatType heartbeatType,
                                                     std::shared_ptr<ClientWorkerRemoteApi> &localWorkerApi,
                                                     std::unique_ptr<client::MmapManager> &localMmapManager,
                                                     std::shared_ptr<client::ListenWorker> &localListenWorker)
{
    localWorkerApi =
        std::make_shared<ClientWorkerRemoteApi>(localAddress, cred_, heartbeatType, token_, signature_.get(), tenantId_,
                                                enableCrossNodeConnection_, enableExclusiveConnection_, deviceId_);
    Status rc = localWorkerApi->Init(requestTimeoutMs_, connectTimeoutMs_, fastTransportMemSize_);
    if (rc.IsError()) {
        LOG(ERROR) << "[Switch] Init preferred same-node worker " << localAddress.ToString()
                   << " failed: " << rc.ToString();
        return rc;
    }

    localMmapManager = std::make_unique<client::MmapManager>(localWorkerApi, false);
    rc = localWorkerApi->PrepairForDecreaseShmRef(std::bind(&client::MmapManager::LookupUnitsAndMmapFd,
                                                            localMmapManager.get(), std::placeholders::_1,
                                                            std::placeholders::_2));
    if (rc.IsError()) {
        LOG(ERROR) << "[Switch] PrepairForDecreaseShmRef for preferred same-node worker failed: " << rc.ToString();
        return rc;
    }

    localListenWorker = std::make_shared<client::ListenWorker>(localWorkerApi, localWorkerApi->heartbeatType_,
                                                               LOCAL_WORKER, asyncSwitchWorkerPool_.get());
    localListenWorker->AddCallBackFunc(this, [this] { ProcessWorkerLost(); });
    localListenWorker->SetWorkerTimeoutHandle([this] { ProcessWorkerTimeout(); });
    localListenWorker->SetReleaseFdCallBack(
        [this](const std::vector<int64_t> &fds) { mmapManager_->ClearExpiredFds(fds); });
    if (enableCrossNodeConnection_) {
        localListenWorker->SetSwitchWorkerHandle([this](uint32_t index, client::SwitchTriggerReason reason) {
            return SwitchWorkerNode(static_cast<WorkerNode>(index), reason);
        });
    }
    localListenWorker->SetIsLocalWorker(true);
    rc = localListenWorker->StartListenWorker();
    if (rc.IsError()) {
        LOG(ERROR) << "[Switch] Start preferred same-node worker listener failed: " << rc.ToString();
        return rc;
    }
    return Status::OK();
}

bool ObjectClientImpl::CommitPreferredLocalWorker(WorkerNode oldNode, const HostPort &localAddress,
                                                  const std::shared_ptr<ClientWorkerRemoteApi> &localWorkerApi,
                                                  std::unique_ptr<client::MmapManager> localMmapManager,
                                                  const std::shared_ptr<client::ListenWorker> &localListenWorker)
{
    // See TrySwitchToLocalSameHost for why the old listener must destruct outside the lock.
    std::shared_ptr<client::ListenWorker> oldLocalListener;
    std::unique_ptr<client::MmapManager> oldMmapManager;
    {
        std::lock_guard<std::mutex> lock(switchNodeMutex_);
        if (currentNode_ == LOCAL_WORKER || currentNode_ != oldNode
            || (clientStateManager_->GetState() & (uint16_t)ClientState::EXITED)) {
            return false;
        }
        ipAddress_ = localAddress;
        workerApi_[LOCAL_WORKER] = localWorkerApi;
        ReplacePreferredLocalWorkerLocked(localMmapManager, oldLocalListener, oldMmapManager);
        listenWorker_[LOCAL_WORKER] = localListenWorker;
        clientEnableP2Ptransfer_ = localWorkerApi->workerEnableP2Ptransfer_;
        memoryRefCount_.SetSupportMultiShmRefCount(localWorkerApi->workerSupportMultiShmRefCount_);
        currentNode_ = LOCAL_WORKER;
        if (listenWorker_[oldNode] != nullptr) {
            listenWorker_[oldNode]->SetSwitched();
        }
        MarkWorkerAvailableLocked();
    }
    return true;
}

bool ObjectClientImpl::RecoverPreferredLocalWorker()
{
    WorkerNode oldNode;
    HostPort localAddress;
    HeartbeatType heartbeatType = HeartbeatType::RPC_HEARTBEAT;
    if (!GetPreferredLocalWorkerToRecover(oldNode, localAddress, heartbeatType)) {
        return false;
    }

    std::shared_ptr<ClientWorkerRemoteApi> localWorkerApi;
    std::unique_ptr<client::MmapManager> localMmapManager;
    std::shared_ptr<client::ListenWorker> localListenWorker;
    auto rc =
        PreparePreferredLocalWorker(localAddress, heartbeatType, localWorkerApi, localMmapManager, localListenWorker);
    if (rc.IsError()) {
        return false;
    }
    if (!CommitPreferredLocalWorker(oldNode, localAddress, localWorkerApi, std::move(localMmapManager),
                                    localListenWorker)) {
        return false;
    }

    LOG(INFO) << "[Switch] Preferred same-node worker recovered at " << localAddress.ToString();
    return true;
}

bool ObjectClientImpl::ReadyToExit(WorkerNode node, const std::shared_ptr<IClientWorkerApi> &workerApi,
                                   const std::shared_ptr<client::ListenWorker> &listenWorker)
{
    if (!workerApi || !listenWorker) {
        return true;
    }

    auto count = workerApi->InvokeCount();
    auto status = listenWorker->CheckWorkerAvailable();
    if (status.IsOk() && count > 0) {
        LOG(INFO) << FormatString("[Switch] Client %d Still have %d invoke count need to process", node, count);
        return false;
    }
    if (status.IsOk()) {
        (void)workerApi->Disconnect(false);
    }
    listenWorker->StopListenWorker(true);
    return true;
}

bool ObjectClientImpl::WaitStandbyWorkerReady(const std::shared_ptr<IClientWorkerApi> &clientWorkerApi)
{
    if (clientWorkerApi == nullptr) {
        LOG(WARNING) << "[Switch] client worker api is nullptr";
        return false;
    }
    LOG(INFO) << FormatString("[Switch] client %s wait for worker %s ready", GetClientId(),
                              clientWorkerApi->hostPort_.ToString());
    constexpr uint64_t maxWaitMilliseconds = 10000;
    constexpr uint64_t waitIntervalMs = 500;
    uint64_t waitMilliseconds = std::min<uint64_t>(clientWorkerApi->heartBeatIntervalMs_ * 2, maxWaitMilliseconds);
    Timer timer;
    bool success = false;
    do {
        success = clientWorkerApi->healthy_;
        if (success || (clientStateManager_->GetState() & (uint16_t)ClientState::EXITED)) {
            break;
        }
        switchPost_.WaitFor(waitIntervalMs);
    } while (timer.ElapsedMilliSecond() <= waitMilliseconds && !success);
    return success;
}

Status ObjectClientImpl::GetAvailableWorkerApi(std::shared_ptr<IClientWorkerApi> &workerApi)
{
    std::lock_guard<std::mutex> lock(switchNodeMutex_);
    if (workerSwitchState_ == WorkerSwitchState::NO_SWITCHABLE_WORKER) {
        return NoSwitchableWorkerStatus();
    }
    WorkerNode id = currentNode_;
    workerApi = workerApi_[id];
    if (workerApi == nullptr) {
        workerApi = workerApi_[LOCAL_WORKER];
        return CheckConnection();
    }
    return CheckConnection(id);
}

Status ObjectClientImpl::GetAvailableWorkerApi(std::shared_ptr<IClientWorkerApi> &workerApi,
                                               std::unique_ptr<Raii> &raii)
{
    std::lock_guard<std::mutex> lock(switchNodeMutex_);
    if (workerSwitchState_ == WorkerSwitchState::NO_SWITCHABLE_WORKER) {
        return NoSwitchableWorkerStatus();
    }
    WorkerNode id = currentNode_;
    workerApi = workerApi_[id];
    if (workerApi == nullptr) {
        workerApi = workerApi_[LOCAL_WORKER];
        RETURN_IF_NOT_OK(CheckConnection());
    } else {
        RETURN_IF_NOT_OK(CheckConnection(id));
    }
    workerApi->IncreaseInvokeCount();
    raii = std::make_unique<Raii>([workerApi]() { workerApi->DecreaseInvokeCount(); });
    return Status::OK();
}

Status ObjectClientImpl::MGetH2D(const std::vector<std::string> &objectKeys,
                                 const std::vector<DeviceBlobList> &devBlobList, std::vector<std::string> &failedKeys,
                                 uint64_t timeoutMs)
{
    PerfPoint perfPoint(PerfKey::HETERO_CLIENT_MGET_H2D);
    AccessRecorder accessPoint(AccessRecorderKey::DS_HETERO_CLIENT_MGETH2D);

    auto rc = CheckMGetH2DInput(objectKeys, devBlobList);
    if (rc.IsError()) {
        failedKeys.clear();
        AccessRecord(accessPoint, rc, devBlobList, objectKeys);
        return rc;
    }
    UpdateClientRemoteH2DConfig(devBlobList[0].deviceIdx);

    auto status = MGetH2DImpl(objectKeys, devBlobList, timeoutMs, failedKeys);
    AccessRecord(accessPoint, status, devBlobList, objectKeys);
    return status;
}

std::shared_future<AsyncResult> ObjectClientImpl::AsyncMGetH2D(const std::vector<std::string> &objectKeys,
                                                               const std::vector<DeviceBlobList> &devBlobList,
                                                               uint64_t timeoutMs)
{
    PerfPoint perfPoint(PerfKey::HETERO_CLIENT_ASYNCMGET_H2D);
    auto accessPoint = std::make_shared<AccessRecorder>(AccessRecorderKey::DS_HETERO_CLIENT_ASYNCMGETH2D);

    auto rc = CheckMGetH2DInput(objectKeys, devBlobList);
    if (rc.IsError()) {
        std::promise<AsyncResult> promise;
        std::shared_future<AsyncResult> future = promise.get_future().share();
        promise.set_value({ rc, {} });
        AccessRecord(*accessPoint, rc, devBlobList, objectKeys);
        return future;
    }

    auto asyncState = std::make_shared<AsyncMGetH2DState>(objectKeys, devBlobList);
    std::shared_future<AsyncResult> future = asyncState->promise.get_future().share();
    UpdateClientRemoteH2DConfig(asyncState->devBlobList[0].deviceIdx);

    auto traceContext = Trace::Instance().GetContext();
    auto asyncStateForRpc = asyncState;
    asyncState->rpcFuture =
        asyncGetRPCPool_->Submit([this, traceContext, timeoutMs, asyncState = std::move(asyncStateForRpc)]() {
            TraceGuard traceGuard = Trace::Instance().SetTraceContext(traceContext);
            PerfPoint point(PerfKey::CLIENT_MGET_FROM_WORKER);
            // MGetH2D supports RH2D transfer, so if RH2D feature is enabled, it can trigger RH2D.
            bool isRH2DSupported = true;
            RETURN_IF_NOT_OK(Get(asyncState->objectKeys, timeoutMs, asyncState->bufferList, false, isRH2DSupported));

            CHECK_FAIL_RETURN_STATUS(asyncState->objectKeys.size() == asyncState->bufferList.size(), K_INVALID,
                                     "The size of objectKeys and bufferList does not match");

            asyncState->existBufferList.reserve(asyncState->bufferList.size());
            std::vector<uint32_t> devices;
            devices.reserve(asyncState->objectKeys.size());
            for (size_t i = 0; i < asyncState->objectKeys.size(); i++) {
                devices.emplace_back(asyncState->devBlobList[i].deviceIdx);
                if (!asyncState->bufferList[i]) {
                    asyncState->failedKeys.emplace_back(asyncState->objectKeys[i]);
                    asyncState->existBufferList.emplace_back(nullptr);
                    continue;
                }
                asyncState->existBufferList.emplace_back(&asyncState->bufferList[i].value());
            }
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CheckDeviceValid(devices), "Check device failed.");
            return Status::OK();
        });

    asyncGetCopyPool_->Execute(
        [this, traceContext, asyncState = std::move(asyncState), accessPoint = std::move(accessPoint)]() mutable {
            TraceGuard traceGuard = Trace::Instance().SetTraceContext(traceContext);
            auto rc = asyncState->rpcFuture.get();
            if (rc.IsOk()) {
                rc = HostDataCopy2Device(asyncState->devBlobList, asyncState->existBufferList);
            }
            AccessRecord(*accessPoint, rc, asyncState->devBlobList, asyncState->objectKeys);
            asyncState->promise.set_value({ rc, asyncState->failedKeys });
        });
    return future;
}

Status ObjectClientImpl::MGetH2DImpl(const std::vector<std::string> &objectKeys,
                                     const std::vector<DeviceBlobList> &devBlobList, uint64_t timeoutMs,
                                     std::vector<std::string> &failedKeys)
{
    PerfPoint point(PerfKey::CLIENT_MGET_FROM_WORKER);
    failedKeys.clear();
    // Hold buffers until HostDataCopy2Device finishes and releases raw pointers.
    std::vector<Optional<Buffer>> bufferList;

    // MGetH2D supports RH2D transfer, so if RH2D feature is enabled, it can trigger RH2D.
    bool isRH2DSupported = true;
    RETURN_IF_NOT_OK(Get(objectKeys, timeoutMs, bufferList, false, isRH2DSupported));

    CHECK_FAIL_RETURN_STATUS(objectKeys.size() == bufferList.size(), K_INVALID,
                             "The size of objectKeys and bufferList does not match");

    std::vector<Buffer *> existBufferList;
    existBufferList.reserve(bufferList.size());
    std::vector<uint32_t> devices;
    devices.reserve(objectKeys.size());
    for (auto i = 0ul; i < objectKeys.size(); i++) {
        devices.emplace_back(devBlobList[i].deviceIdx);
        if (!bufferList[i]) {
            failedKeys.emplace_back(objectKeys[i]);
            existBufferList.emplace_back(nullptr);
            continue;
        }
        existBufferList.emplace_back(&bufferList[i].value());
    }

    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CheckDeviceValid(devices), "Check device failed.");
    std::vector<DeviceBlobList> devBlobListCopy = devBlobList;
    return HostDataCopy2Device(devBlobListCopy, existBufferList);
}

Status ObjectClientImpl::CheckMGetH2DInput(const std::vector<std::string> &objectKeys,
                                           const std::vector<DeviceBlobList> &devBlobList)
{
    if (objectKeys.empty() || devBlobList.empty()) {
        RETURN_STATUS(K_INVALID, FormatString("Got empty parameters : keys nums %zu, blobList nums %zu.",
                                              objectKeys.size(), devBlobList.size()));
    }
    if (objectKeys.size() != devBlobList.size()) {
        RETURN_STATUS(K_INVALID, FormatString("The size of objKeys(%zu) and devBlobList(%zu) does not match",
                                              objectKeys.size(), devBlobList.size()));
    }
    for (const auto &blockList : devBlobList) {
        if (blockList.srcOffset < 0) {
            RETURN_STATUS(K_INVALID,
                          FormatString("Invalid srcOffset: %d, which must be non-negative.", blockList.srcOffset));
        }
    }
    return Status::OK();
}

static Status ImportSegAndReadHostMemory(std::vector<DeviceBlobList *> &devBlobList,
                                         std::vector<Buffer *> &existBufferList)
{
    (void)devBlobList;
    (void)existBufferList;
#ifdef BUILD_HETERO
    // 1. Initialize communicator connection.
    // Note that client uses worker side root info as the key.
    PerfPoint point(PerfKey::CLIENT_IMPORT_SEG_AND_READ);
    RETURN_IF_NOT_OK(RemoteH2DManager::Instance().SetDeviceIdx());
    P2pKind kind = P2P_RECEIVER;
    std::shared_ptr<RemoteH2DContext> p2pComm;
    // Buffers are grouped by data source, so root info should be the same for these objects.
    const auto &rootInfo = existBufferList[0]->GetRemoteHostInfo()->root_info();
    RETURN_IF_NOT_OK(RemoteH2DManager::Instance().P2PCommInitRootInfo(rootInfo.internal(), rootInfo, kind, p2pComm));

    // 2. Import the remote host segment.
    // 3. Read from remote host memory.

    // Initialize vectors to keep entry data in scope
    std::vector<P2pScatterEntry> entries(existBufferList.size());
    std::vector<std::vector<void *>> dstBufs(existBufferList.size());
    std::vector<std::vector<uint64_t>> counts(existBufferList.size());

    // Construct P2pScatterEntries
    for (size_t i = 0; i < existBufferList.size(); i++) {
        auto buffer = existBufferList[i];
        auto *remoteHostInfo = buffer->GetRemoteHostInfo();
        auto &seg = remoteHostInfo->remote_host_segment();
        auto &hostDataInfo = remoteHostInfo->data_info();
        auto &blobs = devBlobList[i]->blobs;
        RETURN_IF_NOT_OK(RemoteH2DManager::Instance().ImportHostSegment(seg));

        auto &entry = entries[i];
        CHECK_FAIL_RETURN_STATUS(
            seg.seg_data_offset() + hostDataInfo.offset() < seg.seg_len(), K_RUNTIME_ERROR,
            FormatString("The offset overflow, starting point:%zu + blob offset:%zu > segment size:%zu",
                         seg.seg_data_offset(), hostDataInfo.offset(), seg.seg_len()));
        entry.ddrBuf = reinterpret_cast<void *>(seg.seg_va() + seg.seg_data_offset() + hostDataInfo.offset());
        entry.numEl = hostDataInfo.sizes_size();
        CHECK_FAIL_RETURN_STATUS(
            entry.numEl == blobs.size() && entry.numEl > 0, K_INVALID,
            FormatString("Blobs count mismatch in devBlobList between sender and receiver, sender count is: %ld, "
                         "receiver count is: %ld, mismatch devBlobList index: %zu, mismatch key index: %zu",
                         entry.numEl, blobs.size(), i, i));
        dstBufs[i].resize(entry.numEl);
        counts[i].resize(entry.numEl);

        for (size_t j = 0; j < entry.numEl; j++) {
            // Double check the sizes and offsets, and prepare the dstBufs and counts for the Get Scatter.
            auto hostDataSize = hostDataInfo.sizes(j);
            auto deviceDataSize = blobs[j].size;
            CHECK_FAIL_RETURN_STATUS(static_cast<size_t>(hostDataSize) == deviceDataSize, K_RUNTIME_ERROR,
                                     "The data size of device and host is not equal.");
            dstBufs[i][j] = blobs[j].pointer;
            counts[i][j] = deviceDataSize;
        }
        HcclDataType dataType = HCCL_DATA_TYPE_UINT8;
        entry.dstBufs = dstBufs[i].data();
        entry.counts = counts[i].data();
        entry.dataType = dataType;
    }

    RETURN_IF_NOT_OK(RemoteH2DManager::Instance().ScatterBatch(entries.data(), entries.size(), p2pComm));
#endif
    return Status::OK();
}

Status ObjectClientImpl::HostDataCopy2Device(std::vector<DeviceBlobList> &devBlobList,
                                             std::vector<Buffer *> &existBufferList)
{
    PerfPoint point(PerfKey::CLIENT_H2D_MEMCPY);
    if (!IsRemoteH2DEnabled()) {
        RETURN_IF_NOT_OK(devOcImpl_->MemCopyBetweenDevAndHost(devBlobList, existBufferList, MemcpyKind::HOST_TO_DEVICE,
                                                              workerApi_[LOCAL_WORKER]->enableHugeTlb_));
    } else {
        // Group buffers by data source in RH2D scenario
        std::vector<DeviceBlobList> localSourceDevBlobList;
        std::vector<Buffer *> localSourceBufferList;
        std::vector<std::vector<DeviceBlobList *>> remoteSourceDevBlobList;
        std::vector<std::vector<Buffer *>> remoteSourceBufferList;
        std::unordered_map<std::string, int> rootInfoToIndexMapping;
        for (size_t i = 0; i < devBlobList.size(); i++) {
            auto &buffer = existBufferList[i];
            // Skip the non-existent buffers
            if (buffer == nullptr) {
                continue;
            }
            if (buffer->GetRemoteHostInfo() == nullptr) {
                localSourceDevBlobList.emplace_back(devBlobList[i]);
                localSourceBufferList.emplace_back(buffer);
                continue;
            }
            const std::string &rootInternal = buffer->GetRemoteHostInfo()->root_info().internal();
            auto iter = rootInfoToIndexMapping.find(rootInternal);
            if (iter == rootInfoToIndexMapping.end()) {
                iter = rootInfoToIndexMapping.emplace(rootInternal, remoteSourceBufferList.size()).first;
                remoteSourceDevBlobList.emplace_back();
                remoteSourceBufferList.emplace_back();
            }
            remoteSourceDevBlobList[iter->second].emplace_back(&devBlobList[i]);
            remoteSourceBufferList[iter->second].emplace_back(buffer);
        }
        if (!localSourceDevBlobList.empty()) {
            RETURN_IF_NOT_OK(devOcImpl_->MemCopyBetweenDevAndHost(localSourceDevBlobList, localSourceBufferList,
                                                                  MemcpyKind::HOST_TO_DEVICE,
                                                                  workerApi_[LOCAL_WORKER]->enableHugeTlb_));
        }
        for (size_t i = 0; i < remoteSourceDevBlobList.size(); i++) {
            RETURN_IF_NOT_OK(ImportSegAndReadHostMemory(remoteSourceDevBlobList[i], remoteSourceBufferList[i]));
        }
    }

    // existBufferList same as bufferList
    point.RecordAndReset(PerfKey::CLIENT_BATCH_BUFFER_DESTRUCT_GET);
    existBufferList.clear();
    return Status::OK();
}

Status ObjectClientImpl::DeviceDataCreate(const std::vector<std::string> &objectKeys,
                                          const std::vector<DeviceBlobList> &devBlobList, const SetParam &setParam,
                                          std::vector<std::shared_ptr<Buffer>> &bufferList, std::vector<bool> &exists)
{
    PerfPoint point(PerfKey::CLIENT_MULTI_CREATE_OBJECT);
    CHECK_FAIL_RETURN_STATUS(!objectKeys.empty(), K_INVALID, "The keys are empty");
    CHECK_FAIL_RETURN_STATUS(objectKeys.size() == devBlobList.size(), K_INVALID,
                             "The size of objectKeys and devBlobList does not match");

    FullParam param;
    param.writeMode = setParam.writeMode;
    param.cacheType = setParam.cacheType;
    std::vector<size_t> dataSizeList;
    dataSizeList.reserve(objectKeys.size());
    for (size_t i = 0; i < devBlobList.size(); i++) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CheckDeviceValid({ static_cast<uint32_t>(devBlobList[i].deviceIdx) }),
                                         "Check device failed.");
    }
    BlobListInfo blobInfo;
    RETURN_IF_NOT_OK(PrepareDataSizeList(dataSizeList, devBlobList, blobInfo));
    LOG(INFO) << blobInfo.ToString(true);
    exists.resize(objectKeys.size(), false);
    RETURN_IF_NOT_OK(MultiCreate(objectKeys, dataSizeList, param, false, bufferList, exists));
    std::vector<std::shared_ptr<Buffer>> filterBufferList;
    std::vector<DeviceBlobList> filterDevBlobList;
    filterBufferList.reserve(objectKeys.size());
    filterDevBlobList.reserve(objectKeys.size());
    for (auto idx = 0u; idx < objectKeys.size(); idx++) {
        CHECK_FAIL_RETURN_STATUS(
            devBlobList[idx].srcOffset >= 0, K_INVALID,
            FormatString("Invalid srcOffset: %d, which must be non-negative.", devBlobList[idx].srcOffset));
        if (exists[idx]) {
            continue;
        }
        filterBufferList.emplace_back(bufferList[idx]);
        filterDevBlobList.emplace_back(devBlobList[idx]);
    }

    bufferList = filterBufferList;
    if (bufferList.empty()) {
        return Status::OK();
    }
    point.RecordAndReset(PerfKey::CLIENT_D2H_MEMCPY);
    ComposeBufferData(bufferList, filterDevBlobList);
    std::vector<Buffer *> bufferRawPtrList;
    bufferRawPtrList.reserve(bufferList.size());
    for (auto &buff : bufferList) {
        bufferRawPtrList.emplace_back(buff.get());
    }
    RETURN_IF_NOT_OK(devOcImpl_->MemCopyBetweenDevAndHost(
        filterDevBlobList, bufferRawPtrList, MemcpyKind::DEVICE_TO_HOST, workerApi_[LOCAL_WORKER]->enableHugeTlb_));

    return Status::OK();
}

Status ObjectClientImpl::MSetD2H(const std::vector<std::string> &objectKeys,
                                 const std::vector<DeviceBlobList> &devBlobList, const SetParam &setParam)
{
    PerfPoint perfPoint(PerfKey::HETERO_CLIENT_MSET_D2H);
    AccessRecorder accessPoint(AccessRecorderKey::DS_HETERO_CLIENT_MSETD2H);
    auto rc = CheckMSetD2HInput(objectKeys, devBlobList, setParam);
    if (rc.IsError()) {
        AccessRecord(accessPoint, rc, devBlobList, objectKeys);
        return rc;
    }
    UpdateClientRemoteH2DConfig(devBlobList[0].deviceIdx);
    auto status = MSetD2HImpl(objectKeys, devBlobList, setParam);
    AccessRecord(accessPoint, status, devBlobList, objectKeys);
    return status;
}

std::shared_future<AsyncResult> ObjectClientImpl::AsyncMSetD2H(const std::vector<std::string> &objectKeys,
                                                               const std::vector<DeviceBlobList> &devBlobList,
                                                               const SetParam &setParam)
{
    PerfPoint perfPoint(PerfKey::HETERO_CLIENT_ASYNCMSET_D2H);
    std::shared_ptr<AccessRecorder> accessPoint =
        std::make_shared<AccessRecorder>(AccessRecorderKey::DS_HETERO_CLIENT_ASYNCMSETD2H);
    auto rc = CheckMSetD2HInput(objectKeys, devBlobList, setParam);
    if (rc.IsError()) {
        std::promise<AsyncResult> promise;
        std::shared_future<AsyncResult> future = promise.get_future().share();
        promise.set_value({ rc, objectKeys });
        AccessRecord(*accessPoint, rc, devBlobList, objectKeys);
        return future;
    }

    auto asyncState = std::make_shared<AsyncMSetD2HState>(objectKeys, devBlobList, setParam);
    UpdateClientRemoteH2DConfig(asyncState->devBlobList[0].deviceIdx);

    auto traceContext = Trace::Instance().GetContext();
    return asyncSetRPCPool_->Submit(
        [this, traceContext, asyncState = std::move(asyncState), accessPoint = std::move(accessPoint)]() mutable {
            TraceGuard traceGuard = Trace::Instance().SetTraceContext(traceContext);
            auto rc = MSetD2HImpl(asyncState->objectKeys, asyncState->devBlobList, asyncState->setParam);
            AccessRecord(*accessPoint, rc, asyncState->devBlobList, asyncState->objectKeys);
            return AsyncResult{ rc, {} };
        });
}

Status ObjectClientImpl::MSetD2HImpl(const std::vector<std::string> &objectKeys,
                                     const std::vector<DeviceBlobList> &devBlobList, const SetParam &setParam)
{
    // Step1: execute Exist check
    std::vector<std::shared_ptr<Buffer>> bufferList;
    std::vector<bool> exists;
    RETURN_IF_NOT_OK(DeviceDataCreate(objectKeys, devBlobList, setParam, bufferList, exists));

    std::vector<uint32_t> devices;
    for (size_t i = 0; i < objectKeys.size(); ++i) {
        if (!exists[i]) {
            devices.emplace_back(devBlobList[i].deviceIdx);
        }
    }
    RETURN_IF_NOT_OK(CheckDeviceValid(devices));

    // If all objects already exist, return success immediately
    if (devices.empty()) {
        return Status::OK();
    }
    // Step3: Execute final MultiPublish operation
    PerfPoint point(PerfKey::CLIENT_MULTI_PUBLISH_OBJECT);
    std::vector<std::vector<std::uint64_t>> blobSizes;
    blobSizes.reserve(devices.size());
    for (size_t i = 0; i < objectKeys.size(); ++i) {
        if (exists[i]) {
            continue;
        }
        const auto &devblob = devBlobList[i];
        std::vector<uint64_t> sizeList;
        sizeList.reserve(devblob.blobs.size());
        for (auto &blob : devblob.blobs) {
            sizeList.emplace_back(blob.size);
        }
        blobSizes.emplace_back(std::move(sizeList));
    }
    return MultiPublish(bufferList, setParam, blobSizes);
}

Status ObjectClientImpl::CheckMSetD2HInput(const std::vector<std::string> &objectKeys,
                                           const std::vector<DeviceBlobList> &devBlobList, const SetParam &setParam)
{
    if (objectKeys.empty() || devBlobList.empty()) {
        RETURN_STATUS(K_INVALID, FormatString("Got empty parameters : keys nums %zu, blobList nums %zu.",
                                              objectKeys.size(), devBlobList.size()));
    }
    if (objectKeys.size() != devBlobList.size()) {
        RETURN_STATUS(K_INVALID, FormatString("The size of objKeys(%zu) and devBlobList(%zu) does not match",
                                              objectKeys.size(), devBlobList.size()));
    }
    if (setParam.writeMode == WriteMode::WRITE_BACK_L2_CACHE
        || setParam.writeMode == WriteMode::WRITE_THROUGH_L2_CACHE) {
        RETURN_STATUS(K_INVALID, FormatString("not support L2 CACHE write mode,current writeMode is %d",
                                              static_cast<int32_t>(setParam.writeMode)));
    }
    auto rc = CheckValidObjectKeyVector(objectKeys);
    if (rc.IsError()) {
        return rc;
    }
    if (!Validator::IsBatchSizeUnderLimit(objectKeys.size())) {
        RETURN_STATUS(K_INVALID, FormatString("The objectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    }
    return Status::OK();
}

bool ObjectClientImpl::IsBufferAlive(uint32_t version)
{
    return CheckConnection().IsOk() && GetWorkerVersion() == version;
}

Status ObjectClientImpl::CheckConnection(WorkerNode id)
{
    if (listenWorker_.size() <= id || listenWorker_[id] == nullptr) {
        return { K_RUNTIME_ERROR,
                 "The current client is abnormal. The listenWorker attribute is empty. Please initialize the client "
                 "again." };
    }
    return listenWorker_[id]->CheckWorkerAvailable();
}

bool ObjectClientImpl::IsScaleDown(WorkerNode id)
{
    if (listenWorker_.size() <= id || listenWorker_[id] == nullptr) {
        return false;
    }
    return listenWorker_[id]->IsWorkerVoluntaryScaleDown();
}

bool ObjectClientImpl::IsHealthy(WorkerNode id)
{
    if (workerApi_.size() <= id || workerApi_[id] == nullptr) {
        return false;
    }
    return workerApi_[id]->healthy_;
}

Status ObjectClientImpl::CheckConnectionWhileShmModify()
{
    RETURN_IF_NOT_OK(CheckConnection());
    return IsClientReady();
}

Status ObjectClientImpl::Create(const std::string &objectKey, uint64_t dataSize, const FullParam &param,
                                std::shared_ptr<Buffer> &buffer)
{
    std::shared_lock<std::shared_timed_mutex> shutdownLck(shutdownMux_);
    RETURN_IF_NOT_OK(IsClientReady());
    CHECK_FAIL_RETURN_STATUS(!objectKey.empty(), K_INVALID, "The objectKey is empty");
    RETURN_IF_NOT_OK(CheckValidObjectKey(objectKey));
    CHECK_FAIL_RETURN_STATUS(dataSize > 0, K_INVALID, "The dataSize value should be bigger than zero.");
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    PerfPoint createPoint(PerfKey::CLIENT_CREATE_OBJECT);
    VLOG(1) << "Begin to create object, object_key: " << objectKey;
    buffer.reset();  // Decrease should precede increase to avoid worker lost (ref cnt will be clear) and then restart.
    std::shared_ptr<Buffer> newBuffer;
    uint32_t version = 0;
    if (workerApi->ShmCreateable(dataSize) || IsUrmaEnabled()) {
        uint64_t metadataSize = 0;
        auto shmBuf = std::make_shared<ShmUnitInfo>();
        std::shared_ptr<UrmaRemoteAddrPb> urmaDataInfo = nullptr;
        Timer timer;
        auto rc = workerApi->Create(objectKey, dataSize, version, metadataSize, shmBuf, urmaDataInfo, param.cacheType);
        const auto elapsedUs = static_cast<uint64_t>(timer.ElapsedMicroSecond());
        const double elapsedMs = static_cast<double>(elapsedUs) / US_PER_MS;
        PLOG_IF_OR_VLOG(INFO, elapsedUs >= CLIENT_LOCAL_OR_RPC_SLOW_US || rc.IsError(), 1,
                        FormatString("Finished creating object to worker, object_key: %s, path: %s, cost: %.3fms, "
                                     "rc: %s",
                                     objectKey, IsUrmaEnabled() && urmaDataInfo != nullptr ? "UB" : "SHM", elapsedMs,
                                     rc.ToString()));
        RETURN_IF_NOT_OK(rc);
        std::shared_ptr<ObjectBufferInfo> bufferInfo = nullptr;
        std::shared_ptr<client::IMmapTableEntry> mmapEntry = nullptr;
        if (!urmaDataInfo) {
            RETURN_IF_NOT_OK(mmapManager_->LookupUnitsAndMmapFd("", shmBuf));
            mmapEntry = mmapManager_->GetMmapEntryByFd(shmBuf->fd);
            CHECK_FAIL_RETURN_STATUS(mmapEntry != nullptr, StatusCode::K_RUNTIME_ERROR, "Get mmap entry failed");
            bufferInfo =
                MakeObjectBufferInfo(objectKey, (uint8_t *)(shmBuf->pointer) + shmBuf->offset, dataSize, metadataSize,
                                     param, false, version, shmBuf->id, nullptr, std::move(mmapEntry));
        } else {
            bufferInfo = MakeObjectBufferInfo(objectKey, nullptr, dataSize, 0, param, false, version, shmBuf->id);
        }
        // Store URMA info for later use in SendBufferViaUb.
        bufferInfo->ubUrmaDataInfo = urmaDataInfo;
        memoryRefCount_.IncreaseRef(shmBuf->id);
        RETURN_IF_NOT_OK(Buffer::CreateBuffer(std::move(bufferInfo), shared_from_this(), newBuffer));
    } else {
        auto bufferInfo = MakeObjectBufferInfo(objectKey, nullptr, dataSize, 0, param, false, version);
        RETURN_IF_NOT_OK(Buffer::CreateBuffer(std::move(bufferInfo), shared_from_this(), newBuffer));
    }
    buffer = std::move(newBuffer);
    createPoint.Record();
    VLOG(1) << "Finished creating object, object_key: " << objectKey;
    return Status::OK();
}

Status ObjectClientImpl::ConstructMultiCreateParam(const std::vector<std::string> &objectKeyList,
                                                   const std::vector<uint64_t> &dataSizeList,
                                                   std::vector<std::shared_ptr<Buffer>> &bufferList,
                                                   std::vector<MultiCreateParam> &multiCreateParamList,
                                                   uint64_t &dataSizeSum)
{
    auto sz = objectKeyList.size();
    CHECK_FAIL_RETURN_STATUS(sz == dataSizeList.size(), K_INVALID,
                             "The length of objectKeyList and dataSizeList should be the same.");
    multiCreateParamList.reserve(sz);
    for (size_t i = 0; i < sz; i++) {
        auto &objectKey = objectKeyList[i];
        auto dataSize = dataSizeList[i];
        CHECK_FAIL_RETURN_STATUS(dataSize > 0, K_INVALID, "The dataSize value should be bigger than zero.");
        dataSizeSum += dataSize;
        multiCreateParamList.emplace_back(i, objectKey, dataSize);
    }
    bufferList.resize(sz);
    return Status::OK();
}

Status ObjectClientImpl::MultiCreate(const std::vector<std::string> &objectKeyList,
                                     const std::vector<uint64_t> &dataSizeList, const FullParam &param,
                                     const bool skipCheckExistence, std::vector<std::shared_ptr<Buffer>> &bufferList,
                                     std::vector<bool> &exists)
{
    std::shared_lock<std::shared_timed_mutex> shutdownLck(shutdownMux_);
    RETURN_IF_NOT_OK(IsClientReady());
    LOG(INFO) << "Start to MultiCreate " << objectKeyList.size();

    std::vector<MultiCreateParam> multiCreateParamList;
    PerfPoint point(PerfKey::CLIENT_MULTI_CREATE_CONSTRUCT_PARAM);
    uint64_t dataSizeSum = 0;
    RETURN_IF_NOT_OK(
        ConstructMultiCreateParam(objectKeyList, dataSizeList, bufferList, multiCreateParamList, dataSizeSum));
    point.Record();
    // If failed with create, need to rollback.
    auto version = 0u;
    // This variable is the output from MultiCreate, indicates whether shared memory was actually used
    auto useShmTransfer = false;
    // Pre-condition check for whether we should attempt shared memory or UB
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    bool canUseShm = workerApi->IsShmEnable() && dataSizeSum >= workerApi->shmThreshold_;
    if (canUseShm || IsUrmaEnabled() || !skipCheckExistence) {
        if (!skipCheckExistence) {
            exists.assign(objectKeyList.size(), false);
        }
        // Call MultiCreate if: 1) using shared memory, OR 2) UB enabled (need urma_info), OR 3) need to check existence
        // When shared memory is unavailable but UB is enabled or we need to check existence, MultiCreate will use RPC
        RETURN_IF_NOT_OK(
            workerApi->MultiCreate(skipCheckExistence, multiCreateParamList, version, exists, useShmTransfer));
    } else {
        // Only skip existence check when explicitly requested AND not using shared memory
        exists.resize(objectKeyList.size(), false);
    }
    if (!useShmTransfer) {
        for (size_t i = 0; i < objectKeyList.size(); i++) {
            if (!skipCheckExistence && exists[i]) {
                auto bufferInfo = MakeObjectBufferInfo(objectKeyList[i], nullptr, 0, 0, param, false, 0);
                std::shared_ptr<Buffer> placeholder;
                RETURN_IF_NOT_OK(Buffer::CreateBuffer(bufferInfo, shared_from_this(), placeholder));
                bufferList[i] = std::move(placeholder);
                continue;
            }
            auto &objectKey = objectKeyList[i];
            auto dataSize = dataSizeList[i];
            auto version = 0u;
            std::shared_ptr<Buffer> newBuffer;
            auto bufferInfo = MakeObjectBufferInfo(objectKey, nullptr, dataSize, 0, param, false, version);
            auto rc = Buffer::CreateBuffer(bufferInfo, shared_from_this(), newBuffer);
            if (rc.IsError()) {
                bufferList.clear();
                return rc;
            }
            bufferList[i] = std::move(newBuffer);
        }
        return Status::OK();
    }
    bool isInactive = false;
    Raii handlerCreateFailed([&isInactive, &bufferList, this]() {
        if (isInactive) {
            return;
        }
        for (const auto &buffer : bufferList) {
            if (buffer == nullptr) {
                continue;
            }
            (void)memoryRefCount_.DecreaseRef(buffer->bufferInfo_->shmId);
        }
        bufferList.clear();
    });
    point.Reset(PerfKey::CLIENT_MULTI_CREATE_RSP_HANDLE);
    RETURN_IF_NOT_OK(MutiCreateParallel(skipCheckExistence, param, version, exists, multiCreateParamList, bufferList));
    isInactive = true;
    return Status::OK();
}

void ObjectClientImpl::BatchReleaseBufferPtr(const std::vector<Buffer *> &buffers)
{
    std::vector<std::pair<ShmKey, std::uint32_t>> shmInfos;

    for (auto &buffer : buffers) {
        if (!buffer || buffer->bufferInfo_->shmId.Empty()) {
            continue;
        }
        shmInfos.emplace_back(buffer->bufferInfo_->shmId, buffer->bufferInfo_->version);
        buffer->isReleased_ = true;
    }
    BatchDecreaseRefCnt(shmInfos);
}

void ObjectClientImpl::BatchDecreaseRefCnt(const std::vector<std::pair<ShmKey, std::uint32_t>> &shmInfos)
{
    auto decreaseRefCnt = [this](const std::vector<std::pair<ShmKey, std::uint32_t>> &shmInfos) {
        std::vector<ShmKey> decreaseShms;
        for (auto &info : shmInfos) {
            if (!IsBufferAlive(info.second)) {
                continue;
            }
            const auto &shmId = info.first;
            if (!memoryRefCount_.DecreaseRef(shmId)) {
                continue;
            }
            decreaseShms.emplace_back(shmId);
        }

        PerfPoint descPoint(PerfKey::CLIENT_BATCH_DECREASE_MEM_REF);
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerApi_[LOCAL_WORKER]->DecreaseWorkerRef(decreaseShms),
                                         "DecreaseReferenceCnt failed.");
        return Status::OK();
    };

    Status rc = decreaseRefCnt(shmInfos);
    if (rc.IsError()) {
        LOG(WARNING) << "Decrease reference failed: " << rc.ToString();
    }
}

void ObjectClientImpl::DecreaseReferenceCnt(const ShmKey &shmId, bool isShm, uint32_t version)
{
    std::shared_lock<std::shared_timed_mutex> lck(shutdownMux_);
    if (asyncReleasePool_ == nullptr || shmId.Empty()) {
        METRIC_INC(metrics::KvMetricId::CLIENT_DEC_REF_SKIPPED_TOTAL);
        return;
    }
    bool async = true;
    INJECT_POINT("client.DecreaseReferenceCnt", [&async](bool value) { async = value; });
    if (async) {
        asyncReleasePool_->Execute([this, shmId, isShm, version] {
            LOG_IF_ERROR(DecreaseReferenceCntImpl(shmId, isShm, version), "DecreaseReferenceCntImpl failed");
        });
    } else {
        LOG_IF_ERROR(DecreaseReferenceCntImpl(shmId, isShm, version), "DecreaseReferenceCntImpl failed");
    }
}

Status ObjectClientImpl::DecreaseReferenceCntImpl(const ShmKey &shmId, bool isShm, uint32_t version)
{
    bool needDecreaseWorkerRef = memoryRefCount_.DecreaseRef(shmId);
    VLOG(1) << FormatString("Try decrease ref count for shmId %s on clientId %s, needDecreaseWorkerRef %d", shmId,
                            workerApi_[LOCAL_WORKER]->clientId_, needDecreaseWorkerRef);
    if (!needDecreaseWorkerRef) {
        METRIC_INC(metrics::KvMetricId::CLIENT_DEC_REF_SKIPPED_TOTAL);
        return Status::OK();
    }
    if (isShm && !IsBufferAlive(version)) {
        METRIC_INC(metrics::KvMetricId::CLIENT_DEC_REF_SKIPPED_TOTAL);
        return Status::OK();
    }
    RETURN_IF_NOT_OK(CheckConnection());
    PerfPoint descPoint(PerfKey::CLIENT_DECREASE_MEM_REF);
    auto checkFunc = std::bind(&ObjectClientImpl::CheckConnectionWhileShmModify, this);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerApi_[LOCAL_WORKER]->DecreaseShmRef(shmId, checkFunc, shutdownMux_),
                                     "DecreaseShmRef failed.");
    return Status::OK();
}

Status ObjectClientImpl::UpdateToken(SensitiveValue &token)
{
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    return workerApi->UpdateToken(token);
}

Status ObjectClientImpl::UpdateAkSk(const std::string &accessKey, SensitiveValue &secretKey)
{
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    return workerApi->UpdateAkSk(accessKey, secretKey);
}

Status ObjectClientImpl::Seal(const std::shared_ptr<ObjectBufferInfo> &bufferInfo,
                              const std::unordered_set<std::string> &nestedObjectKeys, bool isShm)
{
    std::shared_lock<std::shared_timed_mutex> shutdownLck(shutdownMux_);
    RETURN_IF_NOT_OK(IsClientReady());
    PerfPoint sealPoint(PerfKey::CLIENT_SEAL_OBJECT);
    RETURN_IF_NOT_OK(CheckConnection());
    RETURN_IF_NOT_OK(CheckValidObjectKeyVector(nestedObjectKeys, true));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        Validator::IsBatchSizeUnderLimit(nestedObjectKeys.size()), K_INVALID,
        FormatString("The nestedObjectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    const std::string &objectKey = bufferInfo->objectKey;
    if (nestedObjectKeys.find(objectKey) != nestedObjectKeys.end()) {
        RETURN_STATUS(K_UNKNOWN_ERROR, "Nested object references cannot be nested in a loop.");
    }
    VLOG(1) << "Begin to seal object, object_key: " << objectKey;
    PerfPoint rpcPoint(PerfKey::RPC_CLIENT_SEAL_OBJECT);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerApi_[LOCAL_WORKER]->Publish(bufferInfo, isShm, true, nestedObjectKeys),
                                     FormatString("Seal object %s", objectKey));
    rpcPoint.Record();
    VLOG(1) << "Finished sealing object, object_key: " << objectKey;
    sealPoint.Record();
    return Status::OK();
}

Status ObjectClientImpl::Publish(const std::shared_ptr<ObjectBufferInfo> &bufferInfo,
                                 const std::unordered_set<std::string> &nestedObjectKeys, bool isShm)
{
    std::shared_lock<std::shared_timed_mutex> shutdownLck(shutdownMux_);
    RETURN_IF_NOT_OK(IsClientReady());
    PerfPoint perfPoint(PerfKey::CLIENT_PUBLISH_OBJECT);
    RETURN_IF_NOT_OK(CheckValidObjectKeyVector(nestedObjectKeys, true));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        Validator::IsBatchSizeUnderLimit(nestedObjectKeys.size()), K_INVALID,
        FormatString("The nestedObjectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    const std::string &objectKey = bufferInfo->objectKey;
    const uint32_t ttlSecond = bufferInfo->ttlSecond;
    const int existence = bufferInfo->existence;
    VLOG(1) << "Begin to publish object, object_key: " << objectKey << " with ttlSecond = " << ttlSecond;

    bufferInfo->isSeal = false;
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    Timer timer;
    auto rc = workerApi->Publish(bufferInfo, isShm, false, nestedObjectKeys, ttlSecond, existence);
    const auto elapsedUs = static_cast<uint64_t>(timer.ElapsedMicroSecond());
    const double elapsedMs = static_cast<double>(elapsedUs) / US_PER_MS;
    PLOG_IF_OR_VLOG(INFO, elapsedUs >= CLIENT_LOCAL_OR_RPC_SLOW_US || rc.IsError(), 1,
                    FormatString("Finished publishing object to worker, object_key: %s, path: %s, cost: %.3fms, rc: %s",
                                 objectKey, isShm ? "SHM" : (bufferInfo->ubUrmaDataInfo != nullptr ? "UB" : "TCP"),
                                 elapsedMs, rc.ToString()));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rc, FormatString("Publish object %s", objectKey));
    return Status::OK();
}

Status ObjectClientImpl::SendBufferViaUb(const std::shared_ptr<ObjectBufferInfo> &bufferInfo, const void *data,
                                         uint64_t length)
{
    g_isThroughUb = true;
    std::shared_ptr<IClientWorkerApi> api;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(api, raii));
    return api->SendBufferViaUb(bufferInfo, data, length);
}

Status ObjectClientImpl::InvalidateBuffer(const std::string &objectKey)
{
    RETURN_IF_NOT_OK(IsClientReady());
    RETURN_IF_NOT_OK(CheckValidObjectKey(objectKey));
    RETURN_IF_NOT_OK(CheckConnection());
    RETURN_IF_NOT_OK(workerApi_[LOCAL_WORKER]->InvalidateBuffer(objectKey));
    return Status::OK();
}

Status ObjectClientImpl::ProcessShmPut(const std::string &objectKey, const uint8_t *data, uint64_t size,
                                       const FullParam &param, const std::unordered_set<std::string> &nestedObjectKeys,
                                       uint32_t ttlSecond, const std::shared_ptr<IClientWorkerApi> &workerApi,
                                       int existence)
{
    // Create a buffer first.
    auto shmBuf = std::make_shared<ShmUnitInfo>();
    uint32_t version = 0;
    uint64_t metadataSize = 0;
    std::shared_ptr<UrmaRemoteAddrPb> urmaDataInfo = nullptr;  // For Create+MemoryCopy+Publish path with URMA
    RETURN_IF_NOT_OK(workerApi->Create(objectKey, size, version, metadataSize, shmBuf, urmaDataInfo, param.cacheType));
    std::shared_ptr<ObjectBufferInfo> objInfo = nullptr;
    std::shared_ptr<client::IMmapTableEntry> mmapEntry = nullptr;
    if (!urmaDataInfo) {
        RETURN_IF_NOT_OK(mmapManager_->LookupUnitsAndMmapFd("", shmBuf));
        mmapEntry = mmapManager_->GetMmapEntryByFd(shmBuf->fd);
        CHECK_FAIL_RETURN_STATUS(mmapEntry != nullptr, StatusCode::K_RUNTIME_ERROR, "Get mmap entry failed");
        objInfo = MakeObjectBufferInfo(objectKey, (uint8_t *)(shmBuf->pointer) + shmBuf->offset, size, metadataSize,
                                       param, false, version, shmBuf->id, nullptr, std::move(mmapEntry));
    } else {
        objInfo = MakeObjectBufferInfo(objectKey, nullptr, size, 0, param, false, version, shmBuf->id);
    }
    // Store URMA info for later use in SendBufferViaUb
    objInfo->ubUrmaDataInfo = urmaDataInfo;
    std::shared_ptr<Buffer> buffer;

    memoryRefCount_.IncreaseRef(shmBuf->id);
    RETURN_IF_NOT_OK(Buffer::CreateBuffer(objInfo, shared_from_this(), buffer));

    // Copy user data into the shared memory buffer.
    // no need call WLatch, the other thread cannot change before publish.
    reqTimeoutDuration.Init(requestTimeoutMs_);
    RETURN_IF_NOT_OK(buffer->MemoryCopy(data, size));

    // Start to send put request.
    // In this case buffer is local data, but rpc must be locked.:
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerApi->Publish(objInfo, !urmaDataInfo || objInfo->ubDataSentByMemoryCopy,
                                                        false, nestedObjectKeys, ttlSecond, existence),
                                     FormatString("Put object %s", objectKey));
    if (!urmaDataInfo) {
        buffer->SetVisibility(true);
    }
    // Destruct buffer with async
    buffer.reset();
    return Status::OK();
}

Status ObjectClientImpl::Get(const std::vector<std::string> &objKeys, int32_t subTimeoutMs,
                             std::vector<std::shared_ptr<DeviceBuffer>> &buffers, std::vector<std::string> &failedList)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(objKeys.size() == buffers.size(), K_INVALID,
                                         "buffer size and object key size not matching");

    std::vector<datasystem::Future> futureVec;
    RETURN_IF_NOT_OK(AsyncGetDevBuffer(objKeys, buffers, futureVec, std::max(RPC_TIMEOUT, subTimeoutMs), subTimeoutMs));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(objKeys.size() == futureVec.size(), K_INVALID,
                                         "buffer size and future size are not matching");

    Status result = Status::OK();
    for (size_t i = 0; i < objKeys.size(); i++) {
        Status rc = futureVec[i].Get(std::max(RPC_TIMEOUT, subTimeoutMs));
        INJECT_POINT("ObjectClientImpl.Get", [&rc] {
            rc = Status(K_INVALID, "inject error");
            return Status::OK();
        });
        if (rc != Status::OK()) {
            failedList.emplace_back(objKeys[i]);
            result = rc;
        }
    }
    if (failedList.size() < objKeys.size()) {
        result = Status::OK();
    }
    if (result.GetCode() == K_FUTURE_TIMEOUT || result.GetCode() == K_RPC_DEADLINE_EXCEEDED) {
        LOG(ERROR) << "get request timeout,msg:" << result.ToString();
        return Status(K_FUTURE_TIMEOUT, "can't find objects");
    }
    if (result.GetCode() == K_NOT_FOUND) {
        LOG(ERROR) << "get request key not found,msg:" << result.ToString();
        return Status(K_NOT_FOUND, "can't find objects");
    }
    return result;
}

Status ObjectClientImpl::Publish(const std::vector<std::shared_ptr<DeviceBuffer>> &buffers,
                                 std::vector<std::string> &failedList)
{
    Status result = Status::OK();
    for (auto &buffer : buffers) {
        auto rc = buffer->Publish();
        if (rc != Status::OK()) {
            std::string objectKey = buffer->GetObjectKey();
            failedList.emplace_back(objectKey);
            result = rc;
        }
    }
    if (failedList.size() < buffers.size()) {
        result = Status::OK();
    }
    return result;
}

Status ObjectClientImpl::Put(const std::string &objectKey, const uint8_t *data, uint64_t size, const FullParam &param,
                             const std::unordered_set<std::string> &nestedObjectKeys, uint32_t ttlSecond, int existence)
{
    std::shared_lock<std::shared_timed_mutex> shutdownLck(shutdownMux_);
    RETURN_IF_NOT_OK(IsClientReady());
    PerfPoint perfPoint(PerfKey::CLIENT_PUT_OBJECT);
    CHECK_FAIL_RETURN_STATUS(!objectKey.empty(), K_INVALID, "The objectKey should not be empty.");
    RETURN_IF_NOT_OK(CheckValidObjectKey(objectKey));
    CHECK_FAIL_RETURN_STATUS(data != nullptr, K_INVALID, "The data pointer should not be null.");
    CHECK_FAIL_RETURN_STATUS(size > 0, K_INVALID, "The dataSize value should be bigger than zero.");
    CHECK_FAIL_RETURN_STATUS(nestedObjectKeys.find(objectKey) == nestedObjectKeys.end(), K_UNKNOWN_ERROR,
                             "Nested object references cannot be nested in a loop.");
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));

    Timer setTimer;
    LOG(INFO) << FormatString("[Set] Begin, objectKey: %s, clientId: %s, worker: %s, path: %s", objectKey,
                              workerApi->clientId_, workerApi->hostPort_.ToString(),
                              workerApi->ShmCreateable(size) ? "SHM" : "UB");
    bool isShm = workerApi->ShmCreateable(size);
    // Process UB case and shm case together since they share the same Create + MemoryCopy + Publish flow.
    Status rc;
    if (isShm || IsUrmaEnabled()) {
        rc = ProcessShmPut(objectKey, data, size, param, nestedObjectKeys, ttlSecond, workerApi, existence);
    } else {
        // Construct info to put.
        auto objInfo = MakeObjectBufferInfo(objectKey, const_cast<uint8_t *>(data), size, 0, param, false, 0);
        rc = workerApi->Publish(objInfo, isShm, false, nestedObjectKeys, ttlSecond, existence);
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("Put object %s failed: %s", objectKey, rc.ToString());
        }
    }
    const auto totalUs = static_cast<uint64_t>(setTimer.ElapsedMicroSecond());
    PLOG_IF_OR_VLOG(INFO, totalUs >= CLIENT_LOCAL_OR_RPC_SLOW_US || rc.IsError(), 1,
                    FormatString("[Set] Done, objectKey: %s, totalCost: %.3fms, status: %s", objectKey,
                                 static_cast<double>(totalUs) / US_PER_MS, rc.ToString()));
    return rc;
}

#define COMPLETE_FUTURE_WHEN_ERROR_AND_RETURN(func_ret)                         \
    do {                                                                        \
        Status ret = func_ret;                                                  \
        if (ret.IsError()) {                                                    \
            asyncResource->promise.set_value({ ret, asyncResource->failList }); \
            return future;                                                      \
        };                                                                      \
    } while (0)

#define CONDITIONAL_RETURN_FUTURE_AND_PRINT_WHEN_ERROR(cond, err, msg)          \
    do {                                                                        \
        if (!(cond)) {                                                          \
            asyncResource->promise.set_value({ Status(err, msg), objectKeys }); \
            LOG(ERROR) << (msg);                                                \
            return future;                                                      \
        }                                                                       \
    } while (0)

struct PipelineAsyncResource {
    std::future<Status> rpcFuture;
    std::promise<AsyncResult> promise;
    H2DParam h2DParam;
    std::vector<std::string> failList;
};

std::shared_future<AsyncResult> ObjectClientImpl::GetWithOsTransportPipeline(
    const std::vector<std::string> &objectKeys, const std::vector<std::pair<void *, size_t>> &devShmChunk,
    int64_t subTimeoutMs)
{
    auto asyncResource = std::make_shared<PipelineAsyncResource>();
    std::shared_future<AsyncResult> future = asyncResource->promise.get_future().share();

#ifdef BUILD_PIPLN_H2D
    PerfPoint perfPoint(PerfKey::CLIENT_GET_WITH_OS_XPRT_PIPLINE);

    // check args
    CONDITIONAL_RETURN_FUTURE_AND_PRINT_WHEN_ERROR(objectKeys.size() == devShmChunk.size(), K_INVALID,
                                                   "objectKeys size is not equal to devShmChunk size");
    CONDITIONAL_RETURN_FUTURE_AND_PRINT_WHEN_ERROR(
        Validator::IsBatchSizeUnderLimit(objectKeys.size()), K_INVALID,
        FormatString("The objectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    COMPLETE_FUTURE_WHEN_ERROR_AND_RETURN(CheckValidObjectKeyVector(objectKeys));

    // client should be at same site with worker by shmem
    std::shared_ptr<IClientWorkerApi> workerApi = workerApi_[LOCAL_WORKER];
    CONDITIONAL_RETURN_FUTURE_AND_PRINT_WHEN_ERROR(workerApi != nullptr, K_INVALID, "no local worker api");
    workerApi->IncreaseInvokeCount();
    CONDITIONAL_RETURN_FUTURE_AND_PRINT_WHEN_ERROR(workerApi->IsShmEnable(), K_NOT_SUPPORTED,
                                                   "not support pipeline rh2d: shared memory is not enabled");

    uint32_t devId = -1;
    try {
        devId = static_cast<uint32_t>(std::stoi(deviceId_));
    } catch (...) {
        COMPLETE_FUTURE_WHEN_ERROR_AND_RETURN(
            Status(K_NOT_SUPPORTED, "client is not initialized with pipeline device id"));
    }
    // check status
    COMPLETE_FUTURE_WHEN_ERROR_AND_RETURN(IsClientReady());
    COMPLETE_FUTURE_WHEN_ERROR_AND_RETURN(CheckConnection());

    // copy params
    std::vector<OsXprtPipln::DevShmInfo> devInfos;
    for (size_t i = 0; i < objectKeys.size(); i++) {
        devInfos.emplace_back(OsXprtPipln::DevShmInfo{ OsXprtPipln::TargetDeviceType::CUDA, devId, devShmChunk[i].first,
                                                       devShmChunk[i].second });
    }
    asyncResource->h2DParam = H2DParam{
        .subTimeoutMs = subTimeoutMs,
        .objectKeys = objectKeys,
        .devInfos = std::move(devInfos),
    };

    auto traceContext = Trace::Instance().GetContext();
    asyncResource->rpcFuture = asyncGetRPCPool_->Submit([this, asyncResource, traceContext, workerApi]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceContext(traceContext);
        std::unique_ptr<Raii> raii = std::make_unique<Raii>([workerApi]() { workerApi->DecreaseInvokeCount(); });

        // do H2D
        GetRspPb getRsp;
        Status ret = workerApi->PipelineRH2D(asyncResource->h2DParam, getRsp);
        if (ret.IsError()) {
            asyncResource->promise.set_value({ ret, asyncResource->h2DParam.objectKeys });
            return ret;
        }

        // check result
        for (int i = 0; i < getRsp.objects_size(); i++) {
            asyncResource->failList.emplace_back(getRsp.objects(i).object_key());
        }

        Status recvRc(static_cast<StatusCode>(getRsp.last_rc().error_code()), getRsp.last_rc().error_msg());
        if (recvRc.IsError()) {
            LOG(WARNING) << "request to worker may be failed, status:" << recvRc.ToString()
                         << " failed keys:" << VectorToString(asyncResource->failList);
        } else if (!asyncResource->failList.empty()) {
            LOG(WARNING) << "Not all H2D sucess, failed keys:" << VectorToString(asyncResource->failList);
        }
        asyncResource->promise.set_value({ recvRc, asyncResource->failList });
        return recvRc;
    });

    perfPoint.Record();
#else
    (void)objectKeys;
    (void)devShmChunk;
    (void)subTimeoutMs;
    COMPLETE_FUTURE_WHEN_ERROR_AND_RETURN(Status(K_NOT_SUPPORTED, "not build with BUILD_PIPLN_H2D"));
#endif
    return future;
}

#undef COMPLETE_FUTURE_WHEN_ERROR
#undef CONDITIONAL_RETURN_FUTURE_AND_PRINT_WHEN_ERROR

Status ObjectClientImpl::GetWithLatch(const std::vector<std::string> &objectKeys, std::vector<std::string> &vals,
                                      int64_t subTimeoutMs, std::vector<Optional<Buffer>> &buffers, size_t &dataSize)
{
    vals.clear();
    Status rc = Get(objectKeys, subTimeoutMs, buffers);
    for (auto &buffer : buffers) {
        if (buffer) {
            RETURN_IF_NOT_OK(buffer->RLatch());
            vals.emplace_back(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize());
            dataSize += buffer->GetSize();
            RETURN_IF_NOT_OK(buffer->UnRLatch());
        } else {
            vals.emplace_back(nullptr, 0);
        }
    }
    return rc;
}

Status ObjectClientImpl::Get(const std::vector<std::string> &objectKeys, int64_t subTimeoutMs,
                             std::vector<Optional<Buffer>> &buffers, bool queryL2Cache, bool isRH2DSupported)
{
    PerfPoint perfPoint(PerfKey::CLIENT_GET_OBJECT);
    g_isThroughUb = false;
    RETURN_IF_NOT_OK(IsClientReady());
    RETURN_IF_NOT_OK(CheckValidObjectKeyVector(objectKeys));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(objectKeys.size()), K_INVALID,
                                         FormatString("The objectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    std::vector<std::shared_ptr<Buffer>> objectBuffers(objectKeys.size());
    GetParam getParam{ .objectKeys = objectKeys,
                       .subTimeoutMs = subTimeoutMs,
                       .readParams = {},
                       .queryL2Cache = queryL2Cache,
                       .isRH2DSupported = isRH2DSupported };
    Status rc = GetBuffersFromWorker(workerApi, getParam, objectBuffers);
    buffers.clear();
    for (auto &objectBuffer : objectBuffers) {
        if (objectBuffer == nullptr) {
            buffers.emplace_back();
        } else {
            buffers.emplace_back(std::move(*objectBuffer));
        }
    }
    perfPoint.Record();
    VLOG(1) << "Finish to Get objects " << VectorToString(objectKeys);
    return rc;
}

Status ObjectClientImpl::Read(const std::vector<ReadParam> &readParams, std::vector<Optional<Buffer>> &buffers)
{
    PerfPoint perfPoint(PerfKey::CLIENT_READ_OBJECT);
    RETURN_IF_NOT_OK(IsClientReady());
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(readParams.size()), K_INVALID,
                                         FormatString("The objectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    std::vector<std::shared_ptr<Buffer>> objectBuffers(readParams.size());
    std::vector<std::string> objectKeys;
    for (const auto &param : readParams) {
        objectKeys.emplace_back(param.key);
    }
    RETURN_IF_NOT_OK(CheckValidObjectKeyVector(objectKeys));
    GetParam getParam{ .objectKeys = objectKeys, .subTimeoutMs = 0, .readParams = readParams };
    Status rc = GetBuffersFromWorker(workerApi, getParam, objectBuffers);
    buffers.clear();
    for (auto &objectBuffer : objectBuffers) {
        if (objectBuffer == nullptr) {
            buffers.emplace_back();
        } else {
            buffers.emplace_back(std::move(*objectBuffer));
        }
    }
    perfPoint.Record();
    LOG(INFO) << "Finish to Get objects " << VectorToString(objectKeys);
    return rc;
}

Status ObjectClientImpl::SetShmObjectBuffer(const std::string &objectKey, const GetRspPb::ObjectInfoPb &info,
                                            uint32_t version, std::shared_ptr<Buffer> &buffer)
{
    // Validator check ids in Get(objectKeys, subTimeoutMs, buffers)
    std::shared_ptr<client::IMmapTableEntry> mmapEntry;
    uint8_t *pointer;
    RETURN_IF_NOT_OK(MmapShmUnit(info.store_fd(), info.mmap_size(), info.offset(), mmapEntry, pointer));
    FullParam param;
    param.writeMode = WriteMode(info.write_mode());
    param.consistencyType = ConsistencyType(info.consistency_type());
    param.cacheType = CacheType(info.cache_type());
    auto bufferInfo =
        MakeObjectBufferInfo(objectKey, pointer, info.data_size(), info.metadata_size(), param, info.is_seal(), version,
                             ShmKey::Intern(info.shm_id()), nullptr, std::move(mmapEntry));

    // Update shared memory reference count.
    memoryRefCount_.IncreaseRef(ShmKey::Intern(info.shm_id()));
    return Buffer::CreateBuffer(std::move(bufferInfo), shared_from_this(), buffer);
}

Status ObjectClientImpl::MmapShmUnit(int64_t fd, uint64_t mmapSize, ptrdiff_t offset,
                                     std::shared_ptr<client::IMmapTableEntry> &mmapEntry, uint8_t *&pointer)
{
    auto shmBuf = std::make_shared<ShmUnitInfo>();
    shmBuf->fd = fd;
    shmBuf->mmapSize = mmapSize;
    shmBuf->offset = offset;
    PerfPoint mmapPoint(PerfKey::CLIENT_LOOK_UP_MMAP_FD);
    RETURN_IF_NOT_OK(mmapManager_->LookupUnitsAndMmapFd("", shmBuf));
    mmapEntry = mmapManager_->GetMmapEntryByFd(shmBuf->fd);
    CHECK_FAIL_RETURN_STATUS(mmapEntry != nullptr, StatusCode::K_RUNTIME_ERROR, "Get mmap entry failed");
    mmapPoint.Record();
    pointer = static_cast<uint8_t *>(shmBuf->pointer) + shmBuf->offset;
    return Status::OK();
}

std::shared_ptr<ObjectBufferInfo> ObjectClientImpl::MakeObjectBufferInfo(
    const std::string &objectKey, uint8_t *pointer, uint64_t size, uint64_t metaSize, const FullParam &param,
    bool isSeal, uint32_t version, const ShmKey &shmId, const std::shared_ptr<RpcMessage> &payloadPointer,
    std::shared_ptr<client::IMmapTableEntry> mmapEntry, std::shared_ptr<RemoteH2DHostInfoPb> remoteHostInfo)
{
    (void)remoteHostInfo;
    auto bufferInfo = std::make_shared<ObjectBufferInfo>();
    bufferInfo->objectKey = objectKey;
    bufferInfo->shmId = shmId;
    bufferInfo->pointer = pointer;
    bufferInfo->dataSize = size;
    bufferInfo->metadataSize = metaSize;
    bufferInfo->ttlSecond = param.ttlSecond;
    bufferInfo->existence = static_cast<int>(param.existence);
    bufferInfo->objectMode.SetWriteMode(param.writeMode);
    bufferInfo->objectMode.SetConsistencyType(param.consistencyType);
    bufferInfo->objectMode.SetCacheType(param.cacheType);
    bufferInfo->isSeal = isSeal;
    bufferInfo->version = version;
    bufferInfo->payloadPointer = payloadPointer;
    bufferInfo->mmapEntry = std::move(mmapEntry);
    (void)remoteHostInfo;
#ifdef BUILD_HETERO
    bufferInfo->remoteHostInfo = std::move(remoteHostInfo);
#endif
    return bufferInfo;
}

Status ObjectClientImpl::GetBuffersFromWorker(std::shared_ptr<IClientWorkerApi> workerApi, GetParam &getParam,
                                              std::vector<std::shared_ptr<Buffer>> &buffers)
{
    const std::vector<std::string> &objectsNeedToGet = getParam.objectKeys;
    const std::vector<ReadParam> &readParams = getParam.readParams;
    CHECK_FAIL_RETURN_STATUS(buffers.size() == objectsNeedToGet.size(), K_INVALID, "buffers size does not match");

#ifdef USE_URMA
    // For UB mode, pre-fetch object sizes via GetObjMetaInfo and split into batches if needed.
    if (IsUrmaEnabled() && workerApi != nullptr && !workerApi->IsShmEnable()) {
        std::vector<ObjMetaInfo> objMetas;
        std::string tenantId = g_ContextTenantId.empty() ? tenantId_ : g_ContextTenantId;
        Timer metaTimer;
        Status metaRc = workerApi->GetObjMetaInfo(tenantId, objectsNeedToGet, objMetas);
        getParam.ubGetObjMetaElapsedMs = static_cast<int64_t>(metaTimer.ElapsedMilliSecond());
        getParam.ubMetaResolved = true;
        g_isThroughUb = true;
        if (metaRc.IsError()) {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metaRc, "GetObjMetaInfo failed before UB get");
        } else if (objMetas.size() != objectsNeedToGet.size()) {
            LOG(WARNING) << "GetObjMetaInfo object count mismatch: expected " << objectsNeedToGet.size() << " but got "
                         << objMetas.size() << ", fallback to TCP/IP payload before get.";
        } else {
            uint64_t ubMaxGetSize = UrmaManager::Instance().GetUBMaxGetDataSize();
            uint64_t totalSize = 0;
            for (const auto &meta : objMetas) {
                totalSize += meta.objSize;
            }
            if (totalSize <= ubMaxGetSize) {
                // common case: everything fits in one buffer. Pass total batch size and perform a single Get.
                getParam.ubTotalSize = totalSize;
            } else {
                // batch special case: total size exceeds buffer limit. Split into sub-batches.
                return GetBuffersFromWorkerBatched(workerApi, getParam, buffers, objMetas, ubMaxGetSize);
            }
        }
    }
#endif

    GetRspPb rsp;
    std::vector<RpcMessage> payloads;
    uint32_t version = 0;

    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerApi->Get(getParam, version, rsp, payloads), "Get error");

    std::vector<std::string> failedObjectKey;
    RETURN_IF_NOT_OK(
        ProcessGetResponse(objectsNeedToGet, readParams, rsp, version, payloads, buffers, failedObjectKey));

    RETURN_OK_IF_TRUE(objectsNeedToGet.size() > failedObjectKey.size());

    Status recvRc(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
    return recvRc.IsOk() ? Status(K_NOT_FOUND, "Cannot get objects from worker") : recvRc;
}

#ifdef USE_URMA
struct UBGetBatch {
    std::vector<size_t> indices;
    uint64_t totalSize = 0;
};

static std::vector<UBGetBatch> BuildUBGetBatches(const std::vector<ObjMetaInfo> &objMetas, uint64_t ubMaxGetSize)
{
    std::vector<UBGetBatch> batches;
    UBGetBatch currentBatch;

    for (size_t i = 0; i < objMetas.size(); ++i) {
        uint64_t objSize = objMetas[i].objSize;

        if (objSize > ubMaxGetSize) {
            if (!currentBatch.indices.empty()) {
                batches.push_back(std::move(currentBatch));
                currentBatch = UBGetBatch{};
            }
            // Oversized key gets its own TCP-only batch.
            // Pass actual size so PrepareUrmaBuffer rejects it without a redundant GetObjMetaInfo RPC.
            UBGetBatch tcpBatch;
            tcpBatch.indices.push_back(i);
            tcpBatch.totalSize = objSize;
            batches.push_back(std::move(tcpBatch));
            continue;
        }

        if (!currentBatch.indices.empty() && currentBatch.totalSize + objSize > ubMaxGetSize) {
            batches.push_back(std::move(currentBatch));
            currentBatch = UBGetBatch{};
        }

        currentBatch.indices.push_back(i);
        currentBatch.totalSize += objSize;
    }

    if (!currentBatch.indices.empty()) {
        batches.push_back(std::move(currentBatch));
    }
    return batches;
}

Status ObjectClientImpl::GetBuffersFromWorkerBatched(std::shared_ptr<IClientWorkerApi> workerApi,
                                                     const GetParam &getParam,
                                                     std::vector<std::shared_ptr<Buffer>> &buffers,
                                                     const std::vector<ObjMetaInfo> &objMetas, uint64_t ubMaxGetSize)
{
    const auto &objectKeys = getParam.objectKeys;
    const auto &readParams = getParam.readParams;

    auto batches = BuildUBGetBatches(objMetas, ubMaxGetSize);
    LOG(INFO) << "UB batch Get: " << objectKeys.size() << " objects split into " << batches.size() << " batches";

    size_t totalSuccessCount = 0;
    Status lastError;

    for (const auto &batch : batches) {
        std::vector<std::string> subKeys;
        subKeys.reserve(batch.indices.size());
        for (size_t idx : batch.indices) {
            subKeys.push_back(objectKeys[idx]);
        }

        std::vector<ReadParam> subReadParams;
        if (!readParams.empty()) {
            subReadParams.reserve(batch.indices.size());
            for (size_t idx : batch.indices) {
                subReadParams.push_back(readParams[idx]);
            }
        }

        std::vector<std::shared_ptr<Buffer>> subBuffers(batch.indices.size());

        GetParam subGetParam{ .objectKeys = subKeys,
                              .subTimeoutMs = getParam.subTimeoutMs,
                              .readParams = subReadParams,
                              .queryL2Cache = getParam.queryL2Cache,
                              .isRH2DSupported = getParam.isRH2DSupported,
                              .ubTotalSize = batch.totalSize,
                              .ubMetaResolved = true,
                              .ubGetObjMetaElapsedMs = getParam.ubGetObjMetaElapsedMs };

        GetRspPb rsp;
        std::vector<RpcMessage> payloads;
        uint32_t version = 0;

        Status rc = workerApi->Get(subGetParam, version, rsp, payloads);
        if (rc.IsError()) {
            LOG(WARNING) << "Batch Get failed for " << subKeys.size() << " objects: " << rc.ToString();
            lastError = rc;
            continue;
        }

        std::vector<std::string> failedObjectKey;
        rc = ProcessGetResponse(subKeys, subReadParams, rsp, version, payloads, subBuffers, failedObjectKey);
        if (rc.IsError()) {
            LOG(WARNING) << "ProcessGetResponse failed in batch: " << rc.ToString();
            lastError = rc;
            continue;
        }

        for (size_t k = 0; k < batch.indices.size(); ++k) {
            buffers[batch.indices[k]] = std::move(subBuffers[k]);
        }
        totalSuccessCount += (subKeys.size() - failedObjectKey.size());
    }

    RETURN_OK_IF_TRUE(totalSuccessCount > 0);
    return lastError.IsOk() ? Status(K_NOT_FOUND, "Cannot get objects from worker") : lastError;
}
#endif

Status ObjectClientImpl::ProcessGetResponse(const std::vector<std::string> &objectKeys,
                                            const std::vector<ReadParam> &readParams, GetRspPb &rsp, uint32_t version,
                                            std::vector<RpcMessage> &payloads,
                                            std::vector<std::shared_ptr<Buffer>> &buffers,
                                            std::vector<std::string> &failedObjectKey)
{
    size_t shmCount = static_cast<size_t>(rsp.objects().size());
    size_t noShmCount = static_cast<size_t>(rsp.payload_info().size());
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        SIZE_MAX - shmCount >= noShmCount, K_RUNTIME_ERROR,
        FormatString("Sum overflow, shmCount:%zu + noShmCount:%zu > UINT_MAX:%zu", shmCount, noShmCount, SIZE_MAX));
    size_t payloadSum = 0;
    if (noShmCount > 0) {
        for (auto &p : rsp.payload_info()) {
            payloadSum += p.part_index().size();
        }
    }
    CHECK_FAIL_RETURN_STATUS(shmCount + noShmCount == objectKeys.size() && payloadSum == payloads.size(),
                             K_UNKNOWN_ERROR, "The response count in GetRspPb does not match with objects count.");
    RETURN_IF_NOT_OK(GetObjectBuffers(objectKeys, rsp, version, readParams, payloads, buffers, failedObjectKey));

    Status recvRc(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
    if (recvRc.IsError()) {
        LOG(WARNING) << "Get request may have failed, status:" << recvRc.ToString()
                     << " failed id:" << VectorToString(failedObjectKey);
    } else if (!failedObjectKey.empty()) {
        LOG(WARNING) << "Not all expected objects were obtained, failed id:" << VectorToString(failedObjectKey);
    }
    return Status::OK();
}

Status ObjectClientImpl::GetObjectBuffers(const std::vector<std::string> &objectsNeedToGet, const GetRspPb &rsp,
                                          uint32_t version, const std::vector<ReadParam> &readParams,
                                          std::vector<RpcMessage> &payloads,
                                          std::vector<std::shared_ptr<Buffer>> &buffers,
                                          std::vector<std::string> &failedObjectKey)
{
    size_t i = 0;
    size_t j = 0;
    size_t shmCount = static_cast<size_t>(rsp.objects().size());
    size_t noShmCount = static_cast<size_t>(rsp.payload_info().size());
    size_t size = objectsNeedToGet.size();
    for (size_t index = 0; index < size; index++) {
        const std::string &objectKey = objectsNeedToGet[index];
        Status status;
        std::shared_ptr<Buffer> &bufferPtr = buffers[i + j];
        bool isShm = false;
        bool isNoShm = false;
        if (i < shmCount) {
            isShm = rsp.objects(i).object_key().empty() ? index == rsp.objects(i).object_index()
                                                        : objectKey == rsp.objects(i).object_key();
        }
        if (j < noShmCount) {
            isNoShm = rsp.payload_info(j).object_key().empty() ? index == rsp.payload_info(j).object_index()
                                                               : objectKey == rsp.payload_info(j).object_key();
        }
        if (isShm) {
            const GetRspPb::ObjectInfoPb &info = rsp.objects(i);
            i++;
            if (info.store_fd() == -1) {
                failedObjectKey.emplace_back(objectKey);
                continue;
            }
            // Special case for Remote H2D scenario.
            if (info.has_host_info()) {
                status = SetRemoteHostObjectBuffer(objectKey, info, version, bufferPtr);
            } else if (readParams.empty()) {
                status = SetShmObjectBuffer(objectKey, info, version, bufferPtr);
            } else {
                status = SetOffsetReadObjectBuffer(objectKey, info, version, readParams[index].offset,
                                                   readParams[index].size, bufferPtr);
            }
        } else if (isNoShm) {
            const GetRspPb::PayloadInfoPb &payloadInfo = rsp.payload_info(j);
            status = SetNonShmObjectBuffer(objectKey, payloadInfo, version, payloads, bufferPtr);
            j++;
        } else {
            RETURN_STATUS(K_UNKNOWN_ERROR, "Object key does not match with GetRspPb");
        }

        if (status.IsError()) {
            failedObjectKey.emplace_back(objectKey);
            bufferPtr = nullptr;
            LOG(ERROR) << "Failed for " << objectKey << " : " << status.ToString();
        }
    }
    return Status::OK();
}

Status ObjectClientImpl::SetRemoteHostObjectBuffer(const std::string &objectKey, const GetRspPb::ObjectInfoPb &info,
                                                   uint32_t version, std::shared_ptr<Buffer> &buffer)
{
    FullParam param;
    param.writeMode = WriteMode(info.write_mode());
    param.consistencyType = ConsistencyType(info.consistency_type());
    param.cacheType = CacheType(info.cache_type());
    auto hostInfo = std::make_shared<RemoteH2DHostInfoPb>();
    *hostInfo = std::move(info.host_info());
    auto bufferInfo = MakeObjectBufferInfo(objectKey, nullptr, info.data_size(), info.metadata_size(), param,
                                           info.is_seal(), version, {}, nullptr, nullptr, hostInfo);
    return Buffer::CreateBuffer(bufferInfo, shared_from_this(), buffer);
}

Status ObjectClientImpl::SetNonShmObjectBuffer(const std::string &objectKey, const GetRspPb::PayloadInfoPb &payloadInfo,
                                               int version, std::vector<RpcMessage> &payloads,
                                               std::shared_ptr<Buffer> &bufferPtr)
{
    FullParam param;
    param.writeMode = WriteMode(payloadInfo.write_mode());
    param.consistencyType = ConsistencyType(payloadInfo.consistency_type());
    param.cacheType = CacheType(payloadInfo.cache_type());
    int payloadIndexSize = payloadInfo.part_index().size();
    if (payloadIndexSize == 1) {
        std::shared_ptr<RpcMessage> payloadSharedPtr =
            std::make_shared<RpcMessage>(std::move(payloads[payloadInfo.part_index(0)]));
        auto bufferInfo = MakeObjectBufferInfo(objectKey, nullptr, payloadInfo.data_size(), 0, param,
                                               payloadInfo.is_seal(), version, {}, payloadSharedPtr, nullptr);
        return Buffer::CreateBuffer(std::move(bufferInfo), shared_from_this(), bufferPtr);
    } else {
        std::vector<RpcMessage> objectPayloads;
        for (int i = 0; i < payloadIndexSize; i++) {
            auto partIndex = payloadInfo.part_index(i);
            if (partIndex >= payloads.size()) {
                RETURN_STATUS(K_UNKNOWN_ERROR,
                              "The response payload_index in GetRspPb exceeds the response payloads size.");
            }
            objectPayloads.emplace_back(std::move(payloads[partIndex]));
        }
        auto bufferInfo = MakeObjectBufferInfo(objectKey, nullptr, payloadInfo.data_size(), 0, param,
                                               payloadInfo.is_seal(), version, {}, nullptr, nullptr);
        RETURN_IF_NOT_OK(Buffer::CreateBuffer(std::move(bufferInfo), shared_from_this(), bufferPtr));
        size_t offset = 0;
        for (const auto &part : objectPayloads) {
            const auto length = part.Size();
            const auto destSize = std::min(bufferPtr->GetSize() - offset, length);
            if (destSize < length) {
                RETURN_STATUS(
                    StatusCode::K_RUNTIME_ERROR,
                    FormatString(
                        "SetNonShmObjectBuffer failed because the MemoryCopy dst size: %zu smaller than src size: %zu",
                        destSize, length));
            }
            Status status =
                ::datasystem::MemoryCopy(static_cast<uint8_t *>(bufferPtr->MutableData()) + offset, destSize,
                                         static_cast<const uint8_t *>(part.Data()), length, memoryCopyThreadPool_);
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
                status.IsOk(), K_RUNTIME_ERROR, FormatString("Copy data to buffer failed, err: %s", status.ToString()));
            offset += length;
        }
        return Status::OK();
    }
}

Status ObjectClientImpl::SetOffsetReadObjectBuffer(const std::string &objectKey, const GetRspPb::ObjectInfoPb &info,
                                                   uint32_t version, uint64_t offset, uint64_t size,
                                                   std::shared_ptr<Buffer> &buffer)
{
    uint64_t dataSize = static_cast<uint64_t>(info.data_size());
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(offset < dataSize, K_RUNTIME_ERROR,
                                         FormatString("The read offset %zu out of range [0,%zu)", offset, dataSize));
    OffsetInfo offsetInfo(offset, size);
    offsetInfo.AdjustReadSize(dataSize);

    std::shared_ptr<client::IMmapTableEntry> mmapEntry;
    uint8_t *pointer;
    MmapShmUnit(info.store_fd(), info.mmap_size(), info.offset(), mmapEntry, pointer);
    FullParam param;
    param.writeMode = WriteMode(info.write_mode());
    param.consistencyType = ConsistencyType(info.consistency_type());
    param.cacheType = CacheType(info.cache_type());
    auto bufferInfo =
        MakeObjectBufferInfo(objectKey, pointer, info.data_size(), info.metadata_size(), param, info.is_seal(), version,
                             ShmKey::Intern(info.shm_id()), nullptr, std::move(mmapEntry));

    // Update shared memory reference count.
    std::shared_ptr<Buffer> tmpbuffer;
    {
        memoryRefCount_.IncreaseRef(ShmKey::Intern(info.shm_id()));
        RETURN_IF_NOT_OK(Buffer::CreateBuffer(std::move(bufferInfo), shared_from_this(), tmpbuffer));
    }

    auto readBufferInfo = MakeObjectBufferInfo(objectKey, nullptr, offsetInfo.readSize, 0, param, info.is_seal(),
                                               version, {}, nullptr, nullptr);
    RETURN_IF_NOT_OK(Buffer::CreateBuffer(std::move(readBufferInfo), shared_from_this(), buffer));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        buffer->MemoryCopy(static_cast<uint8_t *>(tmpbuffer->MutableData()) + offset, offsetInfo.readSize),
        "Memory copy failed.");
    return Status::OK();
}

Status ObjectClientImpl::GIncreaseRef(const std::vector<std::string> &objectKeys,
                                      std::vector<std::string> &failedObjectKeys, const std::string &remoteClientId)
{
    PerfPoint point(PerfKey::CLIENT_GINCREASE_REFERENCE);
    std::shared_lock<std::shared_timed_mutex> shutdownLck(shutdownMux_);
    RETURN_IF_NOT_OK(IsClientReady());
    RETURN_IF_NOT_OK(CheckValidObjectKeyVector(objectKeys));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(objectKeys.size()), K_INVALID,
                                         FormatString("The objectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(failedObjectKeys.empty(), K_INVALID, "The failedObjectKeys not empty");
    RETURN_IF_NOT_OK(CheckConnection());

    if (!remoteClientId.empty()) {
        CHECK_FAIL_RETURN_STATUS(Validator::IsRegexMatch(simpleIdRe_, remoteClientId), K_INVALID,
                                 "The remoteClientId contains illegal char(s).");
        auto rc = workerApi_[LOCAL_WORKER]->GIncreaseWorkerRef(objectKeys, failedObjectKeys, remoteClientId);
        VLOG(1) << "[Ref] Global ref count GIncreaseRef end" << VectorToString(objectKeys);
        return rc;
    }

    std::map<std::string, GlobalRefInfo> accessorTable;  // Need sorted map to lock tbb data.
    std::shared_lock<std::shared_timed_mutex> lck(globalRefMutex_);
    std::unordered_map<std::string, std::string> objWithTenantIdsToObjKey;
    AddTbbLockForGlobalRefIds(objectKeys, accessorTable, objWithTenantIdsToObjKey);

    std::vector<std::string> firstIncIds;
    VLOG(2) << "[Ref] RunTime GIncreaseRef object list: " << VectorToString(objectKeys);  // vlog level 2 means internal
    for (const auto &kv : accessorTable) {
        auto &accessor = *kv.second.second;
        int count = kv.second.first;
        TbbGlobalRefTable::value_type valuePair(kv.first, count);
        bool result = globalRefCount_.insert(accessor, valuePair);
        if (!result) {
            accessor->second += count;
        }
        if ((accessor->second - count) == 0) {
            firstIncIds.emplace_back(objWithTenantIdsToObjKey[kv.first]);
        }
    }

    RETURN_OK_IF_TRUE(firstIncIds.empty());

    VLOG(1) << "[Ref] Global ref count change from 0 to 1 list: " << VectorToString(firstIncIds);

    auto rc = workerApi_[LOCAL_WORKER]->GIncreaseWorkerRef(firstIncIds, failedObjectKeys);
    if (!failedObjectKeys.empty()) {
        GIncreaseRefRollback(failedObjectKeys, accessorTable);
    }

    // Return ok on partial success.
    return accessorTable.size() > failedObjectKeys.size() ? Status::OK() : rc;
}

std::string ObjectClientImpl::ConstructObjKeyWithTenantId(const std::string &objKey)
{
    std::string objKeyWithTenant = objKey;
    std::string tenantId;
    if (!token_.Empty()) {
        tenantId = "";
    } else if (g_ContextTenantId.empty()) {
        tenantId = tenantId_;
    } else {
        tenantId = g_ContextTenantId;
    }
    if (!tenantId.empty()) {
        objKeyWithTenant = g_ContextTenantId + K_SEPARATOR + objKey;
    }
    return objKeyWithTenant;
}

void ObjectClientImpl::GIncreaseRefRollback(const std::vector<std::string> &rollbackObjectKeys,
                                            std::map<std::string, GlobalRefInfo> &accessorTable)
{
    // Reset fail ref count.
    for (const auto &objectKey : rollbackObjectKeys) {
        auto objWithTenant = ConstructObjKeyWithTenantId(objectKey);
        auto it = accessorTable.find(objWithTenant);
        if (it == accessorTable.end()) {
            LOG(WARNING) << "Unknown object key " << objWithTenant;
            continue;
        }

        int count = it->second.first;
        auto &accessor = *it->second.second;
        accessor->second -= count;
        if (accessor->second <= 0) {
            (void)globalRefCount_.erase(accessor);
        }
    }

    LOG(WARNING) << "[Ref] failed GIncreaseRef objectKeys " << VectorToString(rollbackObjectKeys);
}

Status ObjectClientImpl::ReleaseGRefs(const std::string &remoteClientId)
{
    RETURN_IF_NOT_OK(IsClientReady());
    if (remoteClientId.empty()) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(Validator::IsRegexMatch(simpleIdRe_, remoteClientId), K_INVALID,
                             "The remoteClientId contains illegal char(s).");
    RETURN_IF_NOT_OK(workerApi_[LOCAL_WORKER]->ReleaseGRefs(remoteClientId));
    return Status::OK();
}

Status ObjectClientImpl::GDecreaseRef(const std::vector<std::string> &objectKeys,
                                      std::vector<std::string> &failedObjectKeys, const std::string &remoteClientId)
{
    PerfPoint point(PerfKey::CLIENT_GDECREASE_REFERENCE);
    RETURN_IF_NOT_OK(IsClientReady());
    for (auto &objectKey : objectKeys) {
        RETURN_IF_NOT_OK(CheckValidObjectKey(objectKey));
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(objectKeys.size()), K_INVALID,
                                         FormatString("The objectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(failedObjectKeys.empty(), K_RUNTIME_ERROR, "The failedObjectKeys not empty");
    RETURN_IF_NOT_OK(CheckConnection());

    if (!remoteClientId.empty()) {
        CHECK_FAIL_RETURN_STATUS(Validator::IsRegexMatch(simpleIdRe_, remoteClientId), K_INVALID,
                                 "The remoteClientId contains illegal char(s).");
        auto rc = workerApi_[LOCAL_WORKER]->GDecreaseWorkerRef(objectKeys, failedObjectKeys, remoteClientId);
        VLOG(1) << "[Ref] Global ref count GDecreaseRef end " << VectorToString(objectKeys);
        return rc;
    }

    std::map<std::string, GlobalRefInfo> accessorTable;  // Need sorted map to lock tbb data.
    std::shared_lock<std::shared_timed_mutex> lck(globalRefMutex_);
    std::unordered_map<std::string, std::string> objWithTenantIdsToObjKey;
    AddTbbLockForGlobalRefIds(objectKeys, accessorTable, objWithTenantIdsToObjKey);
    VLOG(2) << "[Ref] RunTime GDecreaseRef object list: " << VectorToString(objectKeys);  // vlog level 2 means internal

    std::vector<std::string> finishDecIds;
    for (const auto &kv : accessorTable) {
        auto &accessor = *kv.second.second;
        int count = kv.second.first;
        if (!(globalRefCount_.find(accessor, kv.first))) {
            LOG(WARNING) << FormatString("The objectKey id (%s) does not exist.", kv.first);
            continue;
        }
        // reference count change from n to 0 or negative.
        if (accessor->second > 0 && accessor->second <= count) {
            finishDecIds.emplace_back(objWithTenantIdsToObjKey[kv.first]);
        }

        if (accessor->second < count) {
            LOG(WARNING) << FormatString("GDecrease %s, dec num is %d, cur num is %d", kv.first, count,
                                         accessor->second);
        }
        accessor->second -= count;
    }

    RETURN_OK_IF_TRUE(finishDecIds.empty());

    VLOG(1) << "[Ref] Global ref count change from 1 to 0 list :" << VectorToString(finishDecIds);
    Status rc = workerApi_[LOCAL_WORKER]->GDecreaseWorkerRef(finishDecIds, failedObjectKeys);
    if (!failedObjectKeys.empty()) {
        GDecreaseRefRollback(failedObjectKeys, accessorTable);
    }

    RemoveZeroGlobalRefByRefTable(finishDecIds, accessorTable);

    // Return ok on partial success.
    return accessorTable.size() > failedObjectKeys.size() ? Status::OK() : rc;
}

void ObjectClientImpl::GDecreaseRefRollback(const std::vector<std::string> &rollbackObjectKeys,
                                            std::map<std::string, GlobalRefInfo> &accessorTable)
{
    // Reset fail ref count.
    for (const auto &objectKey : rollbackObjectKeys) {
        auto objWithTenant = ConstructObjKeyWithTenantId(objectKey);
        auto it = accessorTable.find(objWithTenant);
        if (it == accessorTable.end()) {
            LOG(WARNING) << "Unknown object key " << objWithTenant;
            continue;
        }

        int count = it->second.first;
        auto &accessor = *it->second.second;
        // if not exists in globalRefCount_
        if (accessor.empty()) {
            continue;
        }

        accessor->second += count;
    }

    LOG(WARNING) << "[Ref] failed GDecreaseRef objectKeys " << VectorToString(rollbackObjectKeys);
}

Status ObjectClientImpl::CheckValidObjectKey(const std::string &key)
{
    CHECK_FAIL_RETURN_STATUS(Validator::IsIdFormat(key), K_INVALID,
                             FormatString("The key contains illegal char(s), allowed regex format: %s "
                                          "or the length of key must be no more than 255. Current key: %s, length: %d.",
                                          Validator::objKeyFormat, FormatStringForLog(key), key.size()));
    return Status::OK();
}

void ObjectClientImpl::RemoveZeroGlobalRefByRefTable(const std::vector<std::string> &checkIds,
                                                     std::map<std::string, GlobalRefInfo> &accessorTable)
{
    for (const auto &objectKey : checkIds) {
        auto objWithTenant = ConstructObjKeyWithTenantId(objectKey);
        auto it = accessorTable.find(objWithTenant);
        if (it == accessorTable.end()) {
            LOG(WARNING) << "Unknown object key " << objWithTenant;
            continue;
        }
        auto &accessor = *(it->second.second);
        if (accessor->second <= 0) {
            (void)globalRefCount_.erase(accessor);
        }
    }
}

int ObjectClientImpl::QueryGlobalRefNum(const std::string &objectKey)
{
    if (IsClientReady().IsError()) {
        return -1;
    }
    int gRefNum = 0;
    if (CheckConnection().IsError()) {
        return gRefNum;
    }
    std::unordered_map<std::string, std::vector<std::unordered_set<std::string>>> gRefMap;
    Status rc = workerApi_[LOCAL_WORKER]->QueryGlobalRefNum({ objectKey }, gRefMap);
    if (rc.IsError()) {
        LOG(ERROR) << "Query all objects global reference error";
        return -1;
    }
    auto objRefMap = gRefMap.find(objectKey);
    if (objRefMap == gRefMap.end()) {
        return 0;
    }
    for (const auto &ele : objRefMap->second) {
        if (ele.size() > (size_t)std::numeric_limits<int>::max()
            || std::numeric_limits<int>::max() - (int)ele.size() < gRefNum) {
            return -1;
        }
        gRefNum += (int)ele.size();
    }
    return gRefNum;
}

Status ObjectClientImpl::Delete(const std::vector<std::string> &objectKeys, std::vector<std::string> &failedObjectKeys)
{
    PerfPoint perfPoint(PerfKey::HETERO_CLIENT_DELETE);
    RETURN_IF_NOT_OK(IsClientReady());
    RETURN_IF_NOT_OK(CheckValidObjectKeyVector(objectKeys));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(objectKeys.size()), K_INVALID,
                                         FormatString("The objectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    Status rc = workerApi->Delete(objectKeys, failedObjectKeys);
    if (!failedObjectKeys.empty()) {
        LOG(ERROR) << "Delete failed list " << VectorToString(failedObjectKeys) << ", status:" << rc.ToString();
    }
    return objectKeys.size() > failedObjectKeys.size() ? Status::OK() : rc;
}

void ObjectClientImpl::AddTbbLockForGlobalRefIds(const std::vector<std::string> &objectKeys,
                                                 std::map<std::string, GlobalRefInfo> &accessorTable,
                                                 std::unordered_map<std::string, std::string> &objTenantIdsToObj)
{
    std::for_each(objectKeys.begin(), objectKeys.end(),
                  [this, &accessorTable, &objTenantIdsToObj](const std::string &objKey) {
                      auto objWithTenant = ConstructObjKeyWithTenantId(objKey);
                      auto it = accessorTable.find(objWithTenant);
                      if (it == accessorTable.end()) {
                          objTenantIdsToObj[objWithTenant] = objKey;
                          auto accessorPtr = std::make_shared<TbbGlobalRefTable::accessor>();
                          (void)accessorTable.emplace(objWithTenant, std::make_pair(1, std::move(accessorPtr)));
                      } else {
                          it->second.first++;
                      }
                  });
}

Status ObjectClientImpl::Set(const std::shared_ptr<Buffer> &buffer)
{
    g_isThroughUb = false;
    RETURN_IF_NOT_OK(IsClientReady());
    CHECK_FAIL_RETURN_STATUS(buffer != nullptr, K_INVALID, "The buffer should not be empty.");
    RETURN_IF_NOT_OK(buffer->CheckDeprecated());
    std::shared_lock<std::shared_timed_mutex> shutdownLck(shutdownMux_);
    PerfPoint perfPoint(PerfKey::CLIENT_PUT_OBJECT);
    VLOG(1) << "Start putting buffer";
    return buffer->Publish();
}

Status ObjectClientImpl::MSet(const std::vector<std::shared_ptr<Buffer>> &buffers)
{
    g_isThroughUb = false;
    CHECK_FAIL_RETURN_STATUS(!buffers.empty(), K_INVALID, "The buffer list must not be empty.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(buffers.size()), K_INVALID,
                                         FormatString("The buffer size cannot exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    RETURN_IF_NOT_OK(IsClientReady());
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    const size_t bufferCnt = buffers.size();
    std::vector<std::shared_ptr<ObjectBufferInfo>> bufferInfoList;
    bufferInfoList.reserve(bufferCnt);
    for (size_t i = 0; i < bufferCnt; i++) {
        auto &buffer = buffers[i];
        CHECK_FAIL_RETURN_STATUS(buffers[i] != nullptr, K_INVALID, "The buffer should not be empty.");
        RETURN_IF_NOT_OK(buffer->CheckDeprecated());
        // MCreate NX placeholder: key already exists, dataSize=0, skip publishing
        if (buffer->bufferInfo_->dataSize == 0) {
            continue;
        }
        CHECK_FAIL_RETURN_STATUS(!buffer->bufferInfo_->isSeal, K_OC_ALREADY_SEALED, "Client object is already sealed");
        bufferInfoList.push_back(buffer->bufferInfo_);
    }
    if (bufferInfoList.empty()) {
        return Status::OK();
    }
    const uint32_t ttl = bufferInfoList.front()->ttlSecond;
    // MSet(buffers) is the publish step after MCreate. The existence check was already done
    // during MCreate, so the publish step should always use NONE to avoid the worker-side
    // NTX+NX restriction in distributed master mode.
    PublishParam publishParam{ .isTx = false, .isReplica = false, .existence = ExistenceOpt::NONE, .ttlSecond = ttl };
    MultiPublishRspPb rsp;
    RETURN_IF_NOT_OK(workerApi->MultiPublish(bufferInfoList, publishParam, rsp));
    return HandleShmRefCountAfterMultiPublish(buffers, rsp);
}

Status ObjectClientImpl::Set(const std::string &key, const StringView &val, const SetParam &setParam)
{
    g_isThroughUb = false;
    RETURN_IF_NOT_OK(IsClientReady());
    RETURN_IF_NOT_OK(CheckValidObjectKey(key));
    FullParam param;
    param.writeMode = setParam.writeMode;
    param.consistencyType = ConsistencyType::CAUSAL;
    param.cacheType = setParam.cacheType;
    return Put(key, reinterpret_cast<const uint8_t *>(val.data()), val.size(), param, {}, setParam.ttlSecond,
               static_cast<int>(setParam.existence));
}

Status ObjectClientImpl::Set(const StringView &val, const SetParam &setParam, std::string &key)
{
    std::string tmpKey;
    RETURN_IF_NOT_OK(GenerateKey(tmpKey));

    RETURN_IF_NOT_OK(Set(tmpKey, val, setParam));

    key = std::move(tmpKey);
    return Status::OK();
}

Status ObjectClientImpl::CheckMultiSetInputParamValidationNtx(const std::vector<std::string> &keys,
                                                              const std::vector<StringView> &vals,
                                                              std::vector<std::string> &outFailedKeys,
                                                              std::vector<std::string> &deduplicateKeys,
                                                              std::vector<StringView> &deduplicateVals)
{
    std::unordered_set<std::string_view> keySet;
    keySet.reserve(keys.size());
    CHECK_FAIL_RETURN_STATUS(!keys.empty(), K_INVALID, "The keys should not be empty.");
    CHECK_FAIL_RETURN_STATUS(keys.size() == vals.size(), K_INVALID, "The number of key and value is not the same.");
    RETURN_IF_NOT_OK(CheckValidObjectKey(*keys.begin()));
    for (size_t i = 0; i < keys.size(); ++i) {
        CHECK_FAIL_RETURN_STATUS(!keys[i].empty(), K_INVALID, "The key should not be empty.");
        CHECK_FAIL_RETURN_STATUS(vals[i].data() != nullptr, K_INVALID,
                                 FormatString("The value associated with key %s should not be empty.", keys[i]));
        auto [it, inserted] = keySet.emplace(keys[i]);
        (void)it;
        if (!inserted) {
            LOG(ERROR) << "The input parameter contains duplicate key " << keys[i];
            outFailedKeys.emplace_back(keys[i]);
        }
    }
    if (!outFailedKeys.empty()) {
        for (size_t i = 0; i < keys.size(); ++i) {
            if (keySet.find(keys[i]) == keySet.end()) {
                continue;
            }
            deduplicateKeys.emplace_back(keys[i]);
            deduplicateVals.emplace_back(vals[i]);
            keySet.erase(keys[i]);
        }
    }
    return Status::OK();
}

Status ObjectClientImpl::CheckMultiSetInputParamValidation(const std::vector<std::string> &keys,
                                                           const std::vector<StringView> &vals,
                                                           const ExistenceOpt &existence,
                                                           std::map<std::string, StringView> &kv)
{
    CHECK_FAIL_RETURN_STATUS(existence == ExistenceOpt::NX, K_INVALID,
                             "The MSetTx only supports set not existence key now.");
    CHECK_FAIL_RETURN_STATUS(keys.size() > 0, K_INVALID, "The keys should not be empty.");
    CHECK_FAIL_RETURN_STATUS(keys.size() <= MSET_MAX_KEY_COUNT, K_INVALID,
                             "The maximum size of keys in single operation is 8.");
    CHECK_FAIL_RETURN_STATUS(keys.size() == vals.size(), K_INVALID, "The number of key and value is not the same.");
    std::unordered_set<std::string> keyRecord;
    RETURN_IF_NOT_OK(CheckValidObjectKey(*keys.begin()));
    for (size_t i = 0; i < keys.size(); ++i) {
        CHECK_FAIL_RETURN_STATUS(!keys[i].empty(), K_INVALID, "The key should not be empty.");
        CHECK_FAIL_RETURN_STATUS(vals[i].data() != nullptr, K_INVALID,
                                 FormatString("The value associated with key %s should not be empty.", keys[i]));
        CHECK_FAIL_RETURN_STATUS(kv.find(keys[i]) == kv.end(), K_INVALID,
                                 FormatString("The input parameter contains duplicate key %s.", keys[i]));
        kv[keys[i]] = vals[i];
    }
    return Status::OK();
}

Status ObjectClientImpl::AllocateMemoryForMSet(const std::map<std::string, StringView> &kv, const WriteMode &writeMode,
                                               const std::shared_ptr<IClientWorkerApi> &workerApi,
                                               std::vector<std::shared_ptr<Buffer>> &buffers,
                                               std::vector<std::shared_ptr<ObjectBufferInfo>> &bufferInfo,
                                               const CacheType &cacheType)
{
    FullParam param;
    param.writeMode = writeMode;
    param.consistencyType = ConsistencyType::CAUSAL;
    param.cacheType = cacheType;
    int i = 0;
    for (const auto &keyValue : kv) {
        // if is not transaction, the val  of object master less than 500KB, not ShmCreateable.
        if (!workerApi->ShmCreateable(keyValue.second.size()) && !IsUrmaEnabled()) {
            // Transmit data with payload.
            bufferInfo[i] = MakeObjectBufferInfo(
                keyValue.first, reinterpret_cast<uint8_t *>(const_cast<char *>(keyValue.second.data())),
                keyValue.second.size(), 0, param, false, 0);
            i++;
            continue;
        }
        // Transmit data with share memory.
        auto shmBuf = std::make_shared<ShmUnitInfo>();
        uint32_t version = 0;
        uint64_t metadataSize = 0;
        std::shared_ptr<UrmaRemoteAddrPb> urmaDataInfo = nullptr;  // For Create+MemoryCopy+Publish path with URMA
        RETURN_IF_NOT_OK(workerApi->Create(keyValue.first, keyValue.second.size(), version, metadataSize, shmBuf,
                                           urmaDataInfo, param.cacheType));
        std::shared_ptr<ObjectBufferInfo> objInfo = nullptr;
        std::shared_ptr<client::IMmapTableEntry> mmapEntry = nullptr;
        if (!urmaDataInfo) {
            RETURN_IF_NOT_OK(mmapManager_->LookupUnitsAndMmapFd("", shmBuf));
            mmapEntry = mmapManager_->GetMmapEntryByFd(shmBuf->fd);
            CHECK_FAIL_RETURN_STATUS(mmapEntry != nullptr, StatusCode::K_RUNTIME_ERROR, "Get mmap entry failed");
            objInfo = MakeObjectBufferInfo(keyValue.first, (uint8_t *)(shmBuf->pointer) + shmBuf->offset,
                                           keyValue.second.size(), metadataSize, param, false, version, shmBuf->id,
                                           nullptr, std::move(mmapEntry));
        } else {
            objInfo = MakeObjectBufferInfo(keyValue.first, nullptr, keyValue.second.size(), 0, param, false, version,
                                           shmBuf->id);
        }
        // Store URMA info for later use in SendBufferViaUb
        objInfo->ubUrmaDataInfo = urmaDataInfo;
        RETURN_IF_NOT_OK(Buffer::CreateBuffer(objInfo, shared_from_this(), buffers[i]));
        memoryRefCount_.IncreaseRef(shmBuf->id);
        RETURN_IF_NOT_OK(buffers[i]->MemoryCopy(keyValue.second.data(), keyValue.second.size()));
        bufferInfo[i] = std::move(objInfo);
        i++;
    }
    return Status::OK();
}

Status ObjectClientImpl::MutiCreateParallel(const bool skipCheckExistence, const FullParam &param,
                                            const uint32_t &version, std::vector<bool> &exists,
                                            std::vector<MultiCreateParam> &multiCreateParamList,
                                            std::vector<std::shared_ptr<Buffer>> &bufferList)
{
    const int sz = static_cast<int>(multiCreateParamList.size());
    auto multicreate = [&, this](size_t start, size_t end) {
        for (size_t i = start; i < end; i++) {
            RETURN_IF_NOT_OK(CreateBufferForMultiCreateParamAtIndex(i, skipCheckExistence, param, version, exists,
                                                                    multiCreateParamList, bufferList));
        }
        return Status::OK();
    };
    static const int parallelThreshold = 128;
    bool isParallel = multiCreateParamList.size() > parallelThreshold;
    if (!isParallel || parallismNum_ == 0) {
        return multicreate(0, sz);
    }
    static const int parallism = 4;
    return Parallel::ParallelFor<size_t>(0, multiCreateParamList.size(), multicreate, 0, parallism);
}

Status ObjectClientImpl::CreateBufferForMultiCreateParamAtIndex(size_t index, bool skipCheckExistence,
                                                                const FullParam &param, uint32_t version,
                                                                const std::vector<bool> &exists,
                                                                std::vector<MultiCreateParam> &multiCreateParamList,
                                                                std::vector<std::shared_ptr<Buffer>> &bufferList)
{
    Status injectRC = Status::OK();
    auto &createParam = multiCreateParamList[index];
    if (!skipCheckExistence && exists[createParam.index]) {
        auto bufferInfo = MakeObjectBufferInfo(createParam.objectKey, nullptr, 0, 0, param, false, 0);
        std::shared_ptr<Buffer> placeholder;
        RETURN_IF_NOT_OK(Buffer::CreateBuffer(bufferInfo, shared_from_this(), placeholder));
        bufferList[createParam.index] = std::move(placeholder);
        return Status::OK();
    }
    auto &shmBuf = createParam.shmBuf;
    std::shared_ptr<ObjectBufferInfo> bufferInfo = nullptr;
#ifdef USE_URMA
    if (createParam.urmaDataInfo) {
        bufferInfo = MakeObjectBufferInfo(createParam.objectKey, nullptr, createParam.dataSize, 0, param, false,
                                          version, shmBuf->id);
        bufferInfo->ubUrmaDataInfo = createParam.urmaDataInfo;
    } else
#endif
    {
        PerfPoint mmapPoint(PerfKey::CLIENT_MULTI_CREATE_GET_MMAP);
        RETURN_IF_NOT_OK(mmapManager_->LookupUnitsAndMmapFd("", shmBuf));
        auto mmapEntry = mmapManager_->GetMmapEntryByFd(shmBuf->fd);
        CHECK_FAIL_RETURN_STATUS(mmapEntry != nullptr, StatusCode::K_RUNTIME_ERROR, "Get mmap entry failed");
        mmapPoint.Record();

        bufferInfo = MakeObjectBufferInfo(createParam.objectKey, (uint8_t *)(shmBuf->pointer) + shmBuf->offset,
                                          createParam.dataSize, createParam.metadataSize, param, false, version,
                                          shmBuf->id, nullptr, std::move(mmapEntry));
    }
    PerfPoint refPoint(PerfKey::CLIENT_MEMORY_REF_ADD);
    memoryRefCount_.IncreaseRef(shmBuf->id);
    refPoint.Record();
    INJECT_POINT("ObjectClientImpl.MultiCreate.mmapFailed", [&bufferList, &injectRC](int failedIndex) {
        if (bufferList[failedIndex] != nullptr) {
            injectRC = Status(StatusCode::K_RUNTIME_ERROR, "Set runtime error");
        }
        return Status::OK();
    });
    RETURN_IF_NOT_OK(injectRC);
    PerfPoint point(PerfKey::CLIENT_MULTI_CREATE_BUFFER_CREATE);
    std::shared_ptr<Buffer> newBuffer;
    RETURN_IF_NOT_OK(Buffer::CreateBuffer(std::move(bufferInfo), shared_from_this(), newBuffer));
    bufferList[createParam.index] = std::move(newBuffer);
    return Status::OK();
}

Status ObjectClientImpl::MCreate(const std::vector<std::string> &keys, const std::vector<uint64_t> &sizes,
                                 const FullParam &param, std::vector<std::shared_ptr<Buffer>> &buffers)
{
    RETURN_IF_NOT_OK(IsClientReady());
    CHECK_FAIL_RETURN_STATUS(keys.size() > 0, K_INVALID, "The keys should not be empty.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(keys.size()), K_INVALID,
                                         FormatString("The key size cannot exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    CHECK_FAIL_RETURN_STATUS(keys.size() == sizes.size(), K_INVALID, "The number of key and value is not the same.");
    for (size_t i = 0; i < keys.size(); ++i) {
        CHECK_FAIL_RETURN_STATUS(!keys[i].empty(), K_INVALID, "The key should not be empty.");
        RETURN_IF_NOT_OK(CheckValidObjectKey(keys[i]));
    }
    LOG(INFO) << "Begin to create multiput object." << VectorToString(keys);
    std::vector<bool> exist;
    bool skipCheckExistence = param.existence != ExistenceOpt::NX;
    return MultiCreate(keys, sizes, param, skipCheckExistence, buffers, exist);
}

Status ObjectClientImpl::MemoryCopyParallel(bool isParallel, const std::vector<std::string> &keys,
                                            const std::vector<StringView> &vals, const FullParam &creatParam,
                                            std::vector<std::shared_ptr<Buffer>> &bufferList,
                                            std::vector<std::shared_ptr<ObjectBufferInfo>> &bufferInfoList)
{
    const int sz = static_cast<int>(bufferList.size());
    auto memoryCopy = [&](int start, int end) {
        for (int i = start; i < end; i++) {
            auto &buffer = bufferList[i];
            if (buffer == nullptr) {
                bufferInfoList[i] =
                    MakeObjectBufferInfo(keys[i], reinterpret_cast<uint8_t *>(const_cast<char *>(vals[i].data())),
                                         vals[i].size(), 0, creatParam, false, 0);
                continue;
            }
            RETURN_IF_NOT_OK(buffer->CheckDeprecated());
            CHECK_FAIL_RETURN_STATUS(!buffer->bufferInfo_->isSeal, K_OC_ALREADY_SEALED,
                                     "Client object is already sealed");
            RETURN_IF_NOT_OK(buffer->MemoryCopy(vals[i].data(), vals[i].size()));
            bufferInfoList[i] = buffer->bufferInfo_;
        }
        return Status::OK();
    };
    if (!isParallel || parallismNum_ == 0) {
        return memoryCopy(0, sz);
    }
    int workerNum = parallismNum_;
    size_t chunkSize = 4;
    if (sz <= parallismNum_) {
        workerNum = sz;
        chunkSize = 1;
    }
    return Parallel::ParallelFor<size_t>(0, bufferInfoList.size(), memoryCopy, chunkSize, workerNum);
}

Status ObjectClientImpl::MSet(const std::vector<std::string> &keys, const std::vector<StringView> &vals,
                              const MSetParam &param, std::vector<std::string> &outFailedKeys)
{
    PerfPoint point(PerfKey::CLIENT_MSET_INPUT_CHECK);
    g_isThroughUb = false;
    std::vector<std::string> deduplicateKeys;
    std::vector<StringView> deduplicateVals;
    RETURN_IF_NOT_OK(CheckMultiSetInputParamValidationNtx(keys, vals, outFailedKeys, deduplicateKeys, deduplicateVals));
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    LOG(INFO) << "Begin to multiput object." << VectorToString(keys);
    FullParam creatParam;
    creatParam.writeMode = param.writeMode;
    creatParam.consistencyType = ConsistencyType::CAUSAL;
    creatParam.cacheType = param.cacheType;
    const std::vector<std::string> &filteredKeys = deduplicateKeys.empty() ? keys : deduplicateKeys;
    const std::vector<StringView> &filteredValues = deduplicateVals.empty() ? vals : deduplicateVals;
    point.RecordAndReset(PerfKey::CLIENT_MSET_MULTICREATE);
    std::vector<uint64_t> dataSizeList;
    uint64_t dataSizeSum = 0;
    dataSizeList.reserve(filteredValues.size());
    for (const auto &val : filteredValues) {
        dataSizeList.emplace_back(val.size());
        dataSizeSum += val.size();
    }
    std::vector<std::shared_ptr<Buffer>> bufferList;
    std::vector<bool> exist;
    RETURN_IF_NOT_OK(MultiCreate(filteredKeys, dataSizeList, creatParam, true, bufferList, exist));
    std::vector<std::shared_ptr<ObjectBufferInfo>> bufferInfoList(bufferList.size());
    static const int minSizeThreshold = 500 * KB;
    static const int sizeThreshold = 4 * MB_TO_BYTES;
    static const int countThreshold = 32;
    bool isParallel =
        dataSizeSum > minSizeThreshold && (dataSizeSum >= sizeThreshold || filteredKeys.size() >= countThreshold);
    point.RecordAndReset(PerfKey::CLIENT_MSET_MEMCOPY);
    RETURN_IF_NOT_OK(
        MemoryCopyParallel(isParallel, filteredKeys, filteredValues, creatParam, bufferList, bufferInfoList));
    point.RecordAndReset(PerfKey::CLIENT_MSET_MULTI_PUBLISH);
    MultiPublishRspPb rsp;
    PublishParam publishParam{
        .isTx = false, .isReplica = false, .existence = param.existence, .ttlSecond = param.ttlSecond
    };
    RETURN_IF_NOT_OK(workerApi->MultiPublish(bufferInfoList, publishParam, rsp));
    point.RecordAndReset(PerfKey::CLIENT_MSET_POST_PROCESS);
    auto status = HandleShmRefCountAfterMultiPublish(bufferList, rsp);
    for (const auto &objKey : rsp.failed_object_keys()) {
        outFailedKeys.emplace_back(objKey);
    }
    if (filteredKeys.size() > outFailedKeys.size()) {
        return Status::OK();
    }
    return status;
}

Status ObjectClientImpl::MSet(const std::vector<std::string> &keys, const std::vector<StringView> &vals,
                              const MSetParam &setParam)
{
    g_isThroughUb = false;
    // Validate the effectiveness of parameters.
    RETURN_IF_NOT_OK(IsClientReady());
    std::map<std::string, StringView> kv;
    RETURN_IF_NOT_OK(CheckMultiSetInputParamValidation(keys, vals, setParam.existence, kv));
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));

    // Construct the memory of values sent to worker.
    LOG(INFO) << "Begin to multiput object." << VectorToString(keys);
    std::vector<std::shared_ptr<Buffer>> buffers(keys.size(), nullptr);
    std::vector<std::shared_ptr<ObjectBufferInfo>> bufferInfo(keys.size(), nullptr);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AllocateMemoryForMSet(kv, setParam.writeMode, workerApi, buffers, bufferInfo, setParam.cacheType),
        "AllocateMemoryForMSet failed");
    PublishParam publishParam{
        .isTx = true, .isReplica = false, .existence = setParam.existence, .ttlSecond = setParam.ttlSecond
    };
    MultiPublishRspPb rsp;
    RETURN_IF_NOT_OK(workerApi->MultiPublish(bufferInfo, publishParam, rsp));
    auto status = HandleShmRefCountAfterMultiPublish(buffers, rsp);
    LOG(INFO) << "Finish to multiset.";
    return status;
}

Status ObjectClientImpl::GenerateKey(std::string &key, const std::string &prefixKey)
{
    RETURN_IF_NOT_OK(CheckValidObjectKey(prefixKey));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(IsClientReady(), "Generate key failed.");

    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK_APPEND_MSG(GetAvailableWorkerApi(workerApi, raii), "Generate key failed.");

    auto workerId = workerApi->workerId_;
    CHECK_FAIL_RETURN_STATUS(!workerId.empty(), K_RUNTIME_ERROR, "The worker id is empty!");
    std::string suffix = ";" + workerId;
    if (prefixKey.empty()) {
        key = GetStringUuid() + suffix;
    } else {
        key = prefixKey + suffix;
    }
    return Status::OK();
}

Status ObjectClientImpl::GetPrefix(const std::string &key, std::string &prefix)
{
    std::size_t pos = key.find_last_of(';');
    if (pos != std::string::npos) {
        prefix = key.substr(0, pos);
    } else {
        RETURN_STATUS_LOG_ERROR(K_INVALID, "key is in wrong format: " + key);
    }
    return Status::OK();
}

uint32_t ObjectClientImpl::GetWorkerVersion()
{
    if (CheckConnection().IsError()) {
        return 0;
    }
    return workerApi_[LOCAL_WORKER]->workerVersion_;
}

uint32_t ObjectClientImpl::GetLockId() const
{
    return workerApi_[LOCAL_WORKER]->lockId_;
}

bool ObjectClientImpl::ShmCreateable(uint64_t size) const
{
    return workerApi_[LOCAL_WORKER]->ShmCreateable(size);
}
bool ObjectClientImpl::ShmEnable() const
{
    return workerApi_[LOCAL_WORKER]->IsShmEnable();
}

std::shared_ptr<ThreadPool> ObjectClientImpl::GetMemoryCopyThreadPool()
{
    return memoryCopyThreadPool_;
}

Status ObjectClientImpl::CreateDevBuffer(const std::string &devObjKey, const DeviceBlobList &devBlobList,
                                         const CreateDeviceParam &param, std::shared_ptr<DeviceBuffer> &deviceBuffer)
{
    RETURN_IF_NOT_OK(IsClientReady());
    PerfPoint perfPoint(PerfKey::HETERO_CLIENT_CREATE_DEV_BUFFER);
    return devOcImpl_->CreateDevBuffer(devObjKey, devBlobList, param, deviceBuffer);
}

Status ObjectClientImpl::PublishDeviceObject(std::shared_ptr<DeviceBuffer> buffer)
{
    RETURN_IF_NOT_OK(IsClientReady());
    return devOcImpl_->PublishDeviceObject(std::move(buffer));
}

Status ObjectClientImpl::AsyncGetDevBuffer(const std::vector<std::string> &devObjKeys,
                                           std::vector<std::shared_ptr<DeviceBuffer>> &dstDevBuffers,
                                           std::vector<Future> &futureVec, int64_t prefetchTimeoutMs,
                                           int64_t subTimeoutMs)
{
    RETURN_IF_NOT_OK(IsClientReady());
    return devOcImpl_->AsyncGetDevBuffer(devObjKeys, dstDevBuffers, futureVec, prefetchTimeoutMs, subTimeoutMs);
}

Status ObjectClientImpl::GetSendStatus(const std::shared_ptr<DeviceBuffer> &buffer, std::vector<Future> &futureVec)
{
    RETURN_IF_NOT_OK(IsClientReady());
    return devOcImpl_->GetSendStatus(buffer, futureVec);
}

Status ObjectClientImpl::GetBlobsInfo(const std::string &devObjKey, int32_t timeoutMs, std::vector<Blob> &blobs)
{
    RETURN_IF_NOT_OK(IsClientReady());
    CHECK_FAIL_RETURN_STATUS(!devObjKey.empty(), K_INVALID, "The objectKey is empty");
    RETURN_IF_NOT_OK(CheckValidObjectKey(devObjKey));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        Validator::IsInNonNegativeInt32(timeoutMs), K_INVALID,
        FormatString("timeoutMs %d is out of range., which should be between [%d, %d]", timeoutMs, 0, INT32_MAX));
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    return workerApi->GetBlobsInfo(devObjKey, timeoutMs, blobs);
}

Status ObjectClientImpl::RemoveP2PLocation(const std::string &objectKey, int32_t deviceId)
{
    RETURN_IF_NOT_OK(IsClientReady());
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    return workerApi->RemoveP2PLocation(objectKey, deviceId);
}

Status ObjectClientImpl::GetObjMetaInfo(const std::string &tenantId, const std::vector<std::string> &objectKeys,
                                        std::vector<ObjMetaInfo> &objMetas)
{
    RETURN_IF_NOT_OK(IsClientReady());
    RETURN_IF_NOT_OK(CheckValidObjectKeyVector(objectKeys));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(objectKeys.size() <= OBJ_META_MAX_SIZE_LIMIT, K_INVALID,
                                         FormatString("The objectKeys size exceed %d.", OBJ_META_MAX_SIZE_LIMIT));
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    return workerApi->GetObjMetaInfo(tenantId, objectKeys, objMetas);
}

std::shared_future<AsyncResult> ObjectClientImpl::AsyncDeleteDevObjects(const std::vector<std::string> &objKeys)
{
    auto traceContext = Trace::Instance().GetContext();
    auto accessPoint = std::make_shared<AccessRecorder>(AccessRecorderKey::DS_HETERO_CLIENT_ASYNC_DEVDELETE);
    return asyncDevDeletePool_->Submit([this, traceContext, objKeys, accessPoint]() {
        PerfPoint perfPoint(PerfKey::HETERO_CLIENT_ASYNC_DEV_DELETE_IMPL);
        TraceGuard traceGuard = Trace::Instance().SetTraceContext(traceContext);
        AsyncResult result;
        std::vector<std::string> failList;
        result.status = DeleteDevObjects(objKeys, failList);
        result.failedList = std::move(failList);
        AccessRecord(*accessPoint, result.status, {}, objKeys);
        return result;
    });
}

Status ObjectClientImpl::DeleteDevObjects(const std::vector<std::string> &objKeys, std::vector<std::string> &failList)
{
    RETURN_IF_NOT_OK(IsClientReady());
    RETURN_IF_NOT_OK(CheckValidObjectKeyVector(objKeys));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(objKeys.size()), K_INVALID,
                                         FormatString("The objectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    auto res = workerApi->Delete(objKeys, failList, true);
    if (res.IsError() && failList.empty()) {
        return res;
    }
    for (auto &objKey : objKeys) {
        if (std::find(failList.begin(), failList.end(), objKey) == failList.end()) {
            devOcImpl_->RemoveSubscribe(objKey);
        }
    }
    CHECK_FAIL_RETURN_STATUS(failList.size() < objKeys.size(), res.GetCode(), res.GetMsg());
    return Status::OK();
}

Status ObjectClientImpl::HandleShmRefCountAfterMultiPublish(const std::vector<std::shared_ptr<Buffer>> &bufferList,
                                                            const MultiPublishRspPb &rsp)
{
    Status lastRc(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
    auto failedSet = std::set<std::string>{ rsp.failed_object_keys().begin(), rsp.failed_object_keys().end() };
    for (auto &buffer : bufferList) {
        if (buffer != nullptr && !buffer->bufferInfo_->shmId.Empty()) {
            // If the objectKey is not in the failed set, it means the worker has successfully decreased the reference
            // count. The buffer should not notify the worker again when it is being destructed.
            if (failedSet.find(buffer->bufferInfo_->objectKey) == failedSet.end()) {
                (void)memoryRefCount_.DecreaseRef(buffer->bufferInfo_->shmId);
                buffer->isReleased_ = true;
                buffer->SetVisibility(true);
            }
        }
    }
    // return ok only all objects success
    if (!failedSet.empty() || lastRc.IsError()) {
        LOG(WARNING) << "Cannot set all the objects from worker, status:" << lastRc.ToString()
                     << " failed id:" << VectorToString(failedSet);
        return lastRc.IsOk() ? Status(K_RUNTIME_ERROR, "Some objects set failed in worker") : lastRc;
    }
    return Status::OK();
}

Status ObjectClientImpl::MultiPublish(const std::vector<std::shared_ptr<Buffer>> &bufferList, const SetParam &setParam,
                                      const std::vector<std::vector<uint64_t>> &blobSizes)
{
    std::vector<std::shared_ptr<ObjectBufferInfo>> bufferInfoList;
    bufferInfoList.reserve(bufferList.size());
    for (auto &buffer : bufferList) {
        RETURN_IF_NOT_OK(buffer->CheckDeprecated());
        CHECK_FAIL_RETURN_STATUS(!buffer->bufferInfo_->isSeal, K_OC_ALREADY_SEALED, "Cient object is already sealed");
        bufferInfoList.emplace_back(buffer->bufferInfo_);
    }
    std::shared_lock<std::shared_timed_mutex> shutdownLck(shutdownMux_);
    RETURN_IF_NOT_OK(IsClientReady());
    RETURN_IF_NOT_OK(CheckConnection());

    PublishParam param{
        .isTx = false, .isReplica = true, .existence = setParam.existence, .ttlSecond = setParam.ttlSecond
    };
    MultiPublishRspPb rsp;
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    RETURN_IF_NOT_OK(workerApi->MultiPublish(bufferInfoList, param, rsp, blobSizes));
    return HandleShmRefCountAfterMultiPublish(bufferList, rsp);
}

Status ObjectClientImpl::QuerySize(const std::vector<std::string> &objectKeys, std::vector<uint64_t> &outSizes)
{
    RETURN_IF_NOT_OK(IsClientReady());
    RETURN_IF_NOT_OK(CheckValidObjectKeyVector(objectKeys));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(objectKeys.size() <= QUERY_SIZE_OBJECT_LIMIT, K_INVALID,
                                         FormatString("The objectKeys size exceed %d.", QUERY_SIZE_OBJECT_LIMIT));
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    QuerySizeRspPb rsp;
    Status lastRc;
    outSizes.clear();
    outSizes.reserve(objectKeys.size());
    RETURN_IF_NOT_OK(workerApi->QuerySize(objectKeys, rsp));
    bool isAllZero = true;
    for (auto &size : rsp.sizes()) {
        if (size != 0) {
            isAllZero = false;
        }
        outSizes.emplace_back(size);
    }

    if (!isAllZero) {
        return Status::OK();
    }

    auto recvRc = Status(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
    return recvRc.IsOk() ? Status(StatusCode::K_NOT_FOUND, "All objects are not found!") : recvRc;
}

Status ObjectClientImpl::HealthCheck(ServerState &state)
{
    RETURN_IF_NOT_OK(IsClientReady());
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    return workerApi->HealthCheck(state);
}

Status ObjectClientImpl::DevPublish(const std::vector<std::string> &objectKeys,
                                    const std::vector<DeviceBlobList> &devBlobList, std::vector<Future> &futureVec)
{
    PerfPoint perfPoint(PerfKey::HETERO_CLIENT_PUBLISH);
    CHECK_FAIL_RETURN_STATUS(
        !(objectKeys.empty() || devBlobList.empty()), K_INVALID,
        FormatString("Got empty parameters : keys nums %d, blobList nums %d.", objectKeys.size(), devBlobList.size()));
    CHECK_FAIL_RETURN_STATUS(objectKeys.size() == devBlobList.size(), K_INVALID,
                             "The size of objectKeys and devBlobList does not match");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(objectKeys.size()), K_INVALID,
                                         FormatString("The objectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    RETURN_IF_NOT_OK(CheckValidObjectKeyVector(objectKeys, true));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(objectKeys.size()), K_INVALID,
                                         FormatString("The objectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    std::vector<std::shared_ptr<DeviceBuffer>> devBuffPtrList;
    CreateDeviceParam createParam = CreateDeviceParam{ LifetimeType::MOVE, false };
    RETURN_IF_NOT_OK(ConvertToDevBufferPtrList(objectKeys, devBlobList, createParam, devBuffPtrList));
    Status ret;
    for (auto &ptr : devBuffPtrList) {
        ptr->bufferInfo_->autoRelease = false;
        ret = ptr->Publish();
        if (ret.IsError()) {
            futureVec.clear();
            return ret;
        }
        ret = ptr->GetSendStatus(futureVec);
        if (ret.IsError()) {
            futureVec.clear();
            return ret;
        }
    }
    return Status::OK();
}

Status ObjectClientImpl::DevSubscribe(const std::vector<std::string> &objectKeys,
                                      const std::vector<DeviceBlobList> &devBlobList, std::vector<Future> &futureVec)
{
    PerfPoint perfPoint(PerfKey::HETERO_CLIENT_SUBSCRIBE);
    CHECK_FAIL_RETURN_STATUS(
        !(objectKeys.empty() || devBlobList.empty()), K_INVALID,
        FormatString("Got empty parameters : keys nums %d, blobList nums %d.", objectKeys.size(), devBlobList.size()));
    CHECK_FAIL_RETURN_STATUS(objectKeys.size() == devBlobList.size(), K_INVALID,
                             "The size of objectKeys and devBlobList does not match");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(objectKeys.size()), K_INVALID,
                                         FormatString("The objectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    RETURN_IF_NOT_OK(CheckValidObjectKeyVector(objectKeys, true));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(objectKeys.size()), K_INVALID,
                                         FormatString("The objectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    std::vector<std::shared_ptr<DeviceBuffer>> devBuffPtrList;
    CreateDeviceParam createParam{ LifetimeType::MOVE, false };
    RETURN_IF_NOT_OK(ConvertToDevBufferPtrList(objectKeys, devBlobList, createParam, devBuffPtrList));
    auto ret = AsyncGetDevBuffer(objectKeys, devBuffPtrList, futureVec, RPC_TIMEOUT);
    if (ret.IsError()) {
        futureVec.clear();
        return ret;
    }
    return Status::OK();
}

Status ObjectClientImpl::DevLocalDelete(const std::vector<std::string> &objectKeys,
                                        std::vector<std::string> &failedObjectKeys)
{
    PerfPoint perfPoint(PerfKey::HETERO_CLIENT_LOCAL_DELETE);
    RETURN_IF_NOT_OK(CheckValidObjectKeyVector(objectKeys));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(objectKeys.size()), K_INVALID,
                                         FormatString("The objectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    auto ret = Status::OK();
    for (auto &objectKey : objectKeys) {
        auto res = RemoveP2PLocation(objectKey, ALL_DEVICE_ID);
        INJECT_POINT("ObjectClientImpl.DevLocalDelete", [&res]() {
            res = Status(K_INVALID, "inject error");
            return Status::OK();
        });
        if (res.IsError()) {
            ret = res;
            LOG(ERROR) << FormatString("RemoveP2PLocation error, objectKey:{%s},error msg:{%s}", objectKey,
                                       res.GetMsg());
            failedObjectKeys.emplace_back(objectKey);
            continue;
        }
        devOcImpl_->RemoveSubscribe(objectKey);
    }
    if (failedObjectKeys.size() < objectKeys.size()) {
        return Status::OK();
    }
    return ret;
}

Status ObjectClientImpl::DevMSet(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &blob2dList,
                                 std::vector<std::string> &failedKeys)
{
    PerfPoint perfPoint(PerfKey::HETERO_CLIENT_DEV_MSET);
    CHECK_FAIL_RETURN_STATUS(
        !(keys.empty() || blob2dList.empty()), K_INVALID,
        FormatString("Got empty parameters : keys nums %d, blobList nums %d.", keys.size(), blob2dList.size()));
    CHECK_FAIL_RETURN_STATUS(keys.size() == blob2dList.size(), K_INVALID,
                             "The size of keys and devBlobList does not match");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(keys.size()), K_INVALID,
                                         FormatString("The objectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    RETURN_IF_NOT_OK(CheckValidObjectKeyVector(keys, true));
    std::vector<std::shared_ptr<DeviceBuffer>> devBuffPtrList;
    CreateDeviceParam createParam{ LifetimeType::REFERENCE, true };
    RETURN_IF_NOT_OK(ConvertToDevBufferPtrList(keys, blob2dList, createParam, devBuffPtrList));
    for (auto &devBuff : devBuffPtrList) {
        if (devBuff->Publish().IsError()) {
            failedKeys.emplace_back(devBuff->bufferInfo_->devObjKey);
        };
    }
    return Status::OK();
}

Status ObjectClientImpl::DevMGet(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &blob2dList,
                                 std::vector<std::string> &failedKeys, int32_t timeoutMs)
{
    PerfPoint perfPoint(PerfKey::HETERO_CLIENT_DEV_MGET);
    CHECK_FAIL_RETURN_STATUS(
        !(keys.empty() || blob2dList.empty()), K_INVALID,
        FormatString("Got empty parameters : keys nums %d, blobList nums %d.", keys.size(), blob2dList.size()));
    CHECK_FAIL_RETURN_STATUS(keys.size() == blob2dList.size(), K_INVALID,
                             "The size of objectKeys and blob2dList does not match");
    RETURN_IF_NOT_OK(CheckValidObjectKeyVector(keys, true));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(keys.size()), K_INVALID,
                                         FormatString("The objectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    std::vector<std::shared_ptr<DeviceBuffer>> devBuffPtrList;
    CreateDeviceParam createParam{ LifetimeType::REFERENCE, true };
    RETURN_IF_NOT_OK(ConvertToDevBufferPtrList(keys, blob2dList, createParam, devBuffPtrList));
    RETURN_IF_NOT_OK(Get(keys, timeoutMs, devBuffPtrList, failedKeys));
    return Status::OK();
}

Status ObjectClientImpl::ConvertToDevBufferPtrList(const std::vector<std::string> &keys,
                                                   const std::vector<DeviceBlobList> &blob2dList,
                                                   const CreateDeviceParam &createParam,
                                                   std::vector<std::shared_ptr<DeviceBuffer>> &deviceBuffPtrList)
{
    for (size_t i = 0; i < blob2dList.size(); i++) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CheckDeviceValid({ (uint32_t)blob2dList[i].deviceIdx }),
                                         "Check device failed.");
        CHECK_FAIL_RETURN_STATUS(
            blob2dList[i].srcOffset >= 0, K_INVALID,
            FormatString("Invalid srcOffset: %d, which must be non-negative.", blob2dList[i].srcOffset));
        std::shared_ptr<DeviceBuffer> devBuff;
        RETURN_IF_NOT_OK(CreateDevBuffer(keys[i], blob2dList[i], createParam, devBuff));
        devBuff->bufferInfo_->autoRelease = false;
        devBuff->bufferInfo_->srcOffset = blob2dList[i].srcOffset;
        deviceBuffPtrList.emplace_back(devBuff);
    }
    return Status::OK();
}

Status ObjectClientImpl::CheckDeviceValid(std::vector<uint32_t> deviceId)
{
    PerfPoint point(PerfKey::CLIENT_CHECK_DEVICE_VALID);
    auto *deviceManager = DeviceManagerFactory::GetDeviceManager();
    CHECK_FAIL_RETURN_STATUS(deviceManager != nullptr, K_RUNTIME_ERROR,
                             "No device manager available. Enable heterogeneous support or use the mock test path.");
    return deviceManager->VerifyDeviceId(deviceId);
}

void ObjectClientImpl::StartPerfThread()
{
#ifdef ENABLE_PERF
    if (perfThread_ != nullptr) {
        return;
    }
    LOG(INFO) << "StartPerfThread.";
    perfThread_ = std::make_unique<Thread>([this] {
        const int tickInterval = 1000;
        while (!perfExitFlag_) {
            std::unique_lock<std::mutex> locker(perfMutex_);
            perfCv_.wait_for(locker, std::chrono::milliseconds(tickInterval));
            PerfManager::Instance()->Tick();
        }
        PerfManager::Instance()->PrintPerfLog();
    });
#endif
}

void ObjectClientImpl::StartMetricsThread()
{
    if (!FLAGS_log_monitor || metricsThread_ != nullptr) {
        return;
    }
    LOG(INFO) << "StartMetricsThread.";
    metricsExitFlag_ = false;
    metricsThread_ = std::make_unique<Thread>([this] {
        constexpr int tickIntervalMs = 1000;
        while (!metricsExitFlag_) {
            std::unique_lock<std::mutex> locker(metricsMutex_);
            bool exit = metricsCv_.wait_for(locker, std::chrono::milliseconds(tickIntervalMs),
                                            [this] { return metricsExitFlag_.load(); });
            locker.unlock();
            if (!exit) {
                std::shared_ptr<ThreadPool> pool;
                {
                    std::shared_lock<std::shared_timed_mutex> lck(shutdownMux_);
                    pool = asyncReleasePool_;
                }
                if (pool != nullptr) {
                    metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::CLIENT_ASYNC_RELEASE_QUEUE_SIZE))
                        .Set(static_cast<int64_t>(pool->GetWaitingTasksNum()));
                }
                metrics::Tick();
            }
        }
    });
}

void ObjectClientImpl::StartShmRefReconcileThread()
{
    if (shmRefReconcileThread_ != nullptr) {
        return;
    }
    shmRefReconcileExitFlag_ = false;
    shmRefReconcileExitPost_.Clear();
    shmRefReconcileThread_ = std::make_unique<Thread>([this] { ShmRefReconcileThreadFunc(); });
}

void ObjectClientImpl::ShutdownShmRefReconcileThread()
{
    if (shmRefReconcileThread_ == nullptr) {
        return;
    }
    bool expected = false;
    if (!shmRefReconcileExitFlag_.compare_exchange_strong(expected, true)) {
        return;
    }
    shmRefReconcileExitFlag_ = true;
    shmRefReconcileExitPost_.Set();
    shmRefReconcileThread_->join();
    shmRefReconcileThread_.reset();
}

void ObjectClientImpl::ShmRefReconcileThreadFunc()
{
    constexpr int logIntervalSec = 120;
    std::unordered_set<ShmKey> confirmedExpiredShmIds;
    constexpr size_t reconcileIntervalMs = 5 * 1000UL;
    constexpr size_t minReconcileIntervalMs = 10;
    bool lastRpcFailed = false;
    while (!shmRefReconcileExitFlag_) {
        if (!confirmedExpiredShmIds.empty() || memoryRefCount_.Size() > 0) {
            LOG_EVERY_T(INFO, logIntervalSec)
                << "ShmRefReconcileThreadFunc: size of confirmedExpiredShmIds: " << confirmedExpiredShmIds.size()
                << ",size of memoryRefCount: " << memoryRefCount_.Size();
        }
        auto intervalMs =
            confirmedExpiredShmIds.empty() || lastRpcFailed ? reconcileIntervalMs : minReconcileIntervalMs;
        INJECT_POINT_NO_RETURN("client.shm_ref_reconcile", [&intervalMs](size_t val) { intervalMs = val; });
        (void)shmRefReconcileExitPost_.WaitFor(intervalMs);
        if (shmRefReconcileExitFlag_) {
            break;
        }

        std::shared_ptr<IClientWorkerApi> reconcileWorkerApi;
        {
            std::lock_guard<std::mutex> lock(switchNodeMutex_);
            WorkerNode reconcileWorker = LOCAL_WORKER;
#ifdef USE_URMA
            if (IsUrmaEnabled()) {
                reconcileWorker = currentNode_;
            }
#endif
            if (workerApi_.size() > static_cast<size_t>(reconcileWorker)) {
                reconcileWorkerApi = workerApi_[reconcileWorker];
            }
            if (reconcileWorkerApi == nullptr && workerApi_.size() > static_cast<size_t>(LOCAL_WORKER)) {
                reconcileWorkerApi = workerApi_[LOCAL_WORKER];
            }
        }
        if (reconcileWorkerApi == nullptr) {
            continue;
        }
        std::vector<ShmKey> maybeExpiredShmIds;
        auto rc = reconcileWorkerApi->ReconcileShmRef(confirmedExpiredShmIds, maybeExpiredShmIds);
        lastRpcFailed = rc.IsError();
        if (lastRpcFailed) {
            LOG(WARNING) << "Reconcile shm ref failed: " << rc.ToString();
            continue;
        }
        confirmedExpiredShmIds.clear();
        for (const auto &shmId : maybeExpiredShmIds) {
            if (memoryRefCount_.RefCount(shmId) <= 0) {
                VLOG(1) << "ShmRefReconcileThreadFunc: shmId " << shmId << " has no ref in client " << GetClientId()
                        << ", confirmed expired";
                (void)confirmedExpiredShmIds.emplace(shmId);
            }
        }
    }
}

void ObjectClientImpl::ShutdownPerfThread()
{
#ifdef ENABLE_PERF
    if (perfThread_ == nullptr) {
        return;
    }
    {
        std::unique_lock<std::mutex> locker;
        perfExitFlag_ = true;
        perfCv_.notify_all();
    }
    if (perfThread_->joinable()) {
        perfThread_->join();
    }
#endif
}

void ObjectClientImpl::ShutdownMetricsThread(bool dumpSummary)
{
    std::unique_ptr<Thread> threadToJoin;
    {
        std::lock_guard<std::mutex> locker(metricsMutex_);
        if (metricsThread_ == nullptr) {
            return;
        }
        metricsExitFlag_ = true;
        threadToJoin = std::move(metricsThread_);
    }
    metricsCv_.notify_all();
    if (threadToJoin->joinable()) {
        threadToJoin->join();
    }
    if (dumpSummary) {
        metrics::PrintSummary();
    }
}

Status ObjectClientImpl::Exist(const std::vector<std::string> &keys, std::vector<bool> &exists, const bool queryEtcd,
                               const bool isLocal)
{
    PerfPoint perfPoint(PerfKey::HETERO_CLIENT_EXIST);
    RETURN_IF_NOT_OK(IsClientReady());
    RETURN_IF_NOT_OK(CheckValidObjectKeyVector(keys));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(keys.size() <= QUERY_SIZE_OBJECT_LIMIT, K_INVALID,
                                         FormatString("The objectKeys size exceed %d.", QUERY_SIZE_OBJECT_LIMIT));
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    Timer timer;
    auto rc = workerApi->Exist(keys, exists, queryEtcd, isLocal);
    const auto elapsedUs = static_cast<uint64_t>(timer.ElapsedMicroSecond());
    const double elapsedMs = static_cast<double>(elapsedUs) / US_PER_MS;
    const auto &firstKey = keys.empty() ? "" : keys[0];
    PLOG_IF_OR_VLOG(INFO, elapsedUs >= CLIENT_LOCAL_OR_RPC_SLOW_US || rc.IsError(), 1,
                    FormatString("Finished check exist from worker, first object_key: %s, cost: %.3fms, rc: %s",
                                 firstKey, elapsedMs, rc.ToString()));
    return rc;
}

Status ObjectClientImpl::Expire(const std::vector<std::string> &keys, uint32_t ttlSeconds,
                                std::vector<std::string> &failedKeys)
{
    PerfPoint perfPoint(PerfKey::CLIENT_EXPIRE_OBJECT);
    RETURN_IF_NOT_OK(IsClientReady());
    RETURN_IF_NOT_OK(CheckValidObjectKeyVector(keys));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(keys.size() <= QUERY_SIZE_OBJECT_LIMIT, K_INVALID,
                                         FormatString("The objectKeys size exceed %d.", QUERY_SIZE_OBJECT_LIMIT));
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerApi->Expire(keys, ttlSeconds, failedKeys), "Set expire ttl failed");
    perfPoint.Record();
    return Status::OK();
}

Status ObjectClientImpl::GetMetaInfo(const std::vector<std::string> &keys, const bool isDevKey,
                                     std::vector<MetaInfo> &metaInfos, std::vector<std::string> &failKeys)
{
    RETURN_IF_NOT_OK(IsClientReady());
    RETURN_IF_NOT_OK(CheckValidObjectKeyVector(keys));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(keys.size() <= QUERY_SIZE_OBJECT_LIMIT, K_INVALID,
                                         FormatString("The objectKeys size exceed %d.", QUERY_SIZE_OBJECT_LIMIT));
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    GetMetaInfoRspPb rsp;
    RETURN_IF_NOT_OK(workerApi->GetMetaInfo(keys, isDevKey, rsp));
    auto idx = 0;
    for (const auto &info : rsp.dev_meta_infos()) {
        metaInfos.emplace_back(MetaInfo{ .blobSizeList = { info.blob_sizes().begin(), info.blob_sizes().end() } });
        if (info.blob_sizes().empty()) {
            failKeys.emplace_back(keys[idx]);
        }
        idx++;
    }
    if (!failKeys.empty() && failKeys.size() == keys.size()) {
        return Status(K_NOT_FOUND, "Key not found");
    }
    return Status::OK();
}

Status ObjectClientImpl::UpdateClientRemoteH2DConfig(int32_t devId)
{
    if (devId_ >= 0 && devId_ != devId) {
        LOG(WARNING) << "The client device id is changing from " << devId_ << " to " << devId;
        devId_ = devId;
    }
    SetClientRemoteH2DConfig(enableRemoteH2D_, devId);
    return Status::OK();
}

std::string ObjectClientImpl::GetTransportType() const
{
    if (g_isThroughUb) {
        return "UB";
    }
    return "SHM";
}

}  // namespace object_cache
}  // namespace datasystem
