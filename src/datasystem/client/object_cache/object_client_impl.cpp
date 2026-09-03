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
 * Description: Data system Object Client implementation.
 */

#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/client/object_cache/worker_failover.h"
#include "datasystem/client/object_cache/bound_mode.h"

#include "datasystem/protos/object_posix.brpc.stub.pb.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <bthread/mutex.h>
#include <tbb/concurrent_hash_map.h>

#include "datasystem/client/client_flags_monitor.h"
#include "datasystem/client/mmap_manager/immap_table_entry.h"
#include "datasystem/client/object_cache/routing/routing.h"
#include "datasystem/client/object_cache/transport/common/deadline_retry.h"
#include "datasystem/client/object_cache/transport/object_buffer_internal.h"
#include "datasystem/client/object_cache/transport/transport_layer.h"
#include "datasystem/client/object_cache/transport/worker_snapshot.h"
#include "datasystem/client/object_cache/client_worker_api/iclient_worker_api.h"
#include "datasystem/client/object_cache/exist_handler.h"
#include "datasystem/client/object_cache/direct_receive_buffer_owner.h"
#include "datasystem/client/object_cache/routing/broken_filter.h"
#include "datasystem/client/object_cache/routing/hash_ring_refresher.h"
#include "datasystem/client/object_cache/routing/worker_router.h"
#include "datasystem/common/device/device_manager_factory.h"
#include "datasystem/common/device/device_helper.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/metrics/metrics.h"
#include "datasystem/common/object_cache/buffer_composer.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/object_cache/provider_ub_failure_detail.h"
#include "datasystem/common/log/latency_phase.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/log_sampler.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/log/operation_logger.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/log/spdlog/provider.h"
#include "datasystem/common/parallel/parallel_for.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#ifdef USE_URMA
#include "datasystem/common/rdma/urma_manager.h"
#endif
#ifdef USE_NPU
#include "datasystem/common/device/ascend/acl_device_manager.h"
#include "datasystem/common/rdma/npu/remote_h2d_manager.h"
#endif
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/rpc/brpc_status_util.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/rpc/timeout_duration.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/flags/dynamic_config_updater.h"
#include "datasystem/common/flags/dynamic_flag_config.h"
#include "datasystem/common/util/version.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/rpc_diagnostic.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/request_context.h"
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
DS_DECLARE_int32(fd_pool_prewarm_size);

static constexpr size_t OBJ_META_MAX_SIZE_LIMIT = 64;

namespace datasystem {
namespace {

}  // namespace
}  // namespace datasystem

static constexpr size_t QUERY_SIZE_OBJECT_LIMIT = 10000;
const std::string CLIENT_PARALLEL_THREAD_MIN_NUM_ENV = "CLIENT_PARALLEL_THREAD_MIN_NUM";
const std::string CLIENT_PARALLEL_THREAD_MAX_NUM_ENV = "CLIENT_PARALLEL_THREAD_MAX_NUM";
const std::string CLIENT_MEMORY_COPY_THREAD_NUM_ENV = "CLIENT_MEMORY_COPY_THREAD_NUM";
const std::string CLIENT_MEMORY_COPY_THREAD_NUM_PER_KEY_ENV = "CLIENT_MEMORY_COPY_THREAD_NUM_PER_KEY";
const std::string CLIENT_MEMCOPY_PARALLEL_THRESHOLD_ENV = "CLIENT_MEMCOPY_PARALLEL_THRESHOLD";
const std::string DATASYSTEM_SET_MEMCOPY_THREAD_NUM_ENV = "DATASYSTEM_SET_MEMCOPY_THREAD_NUM";
const std::string DATASYSTEM_SET_MEMCOPY_PARALLEL_THRESHOLD_ENV = "DATASYSTEM_SET_MEMCOPY_PARALLEL_THRESHOLD";
// Keep enough parallelism for the high-throughput SET path.  The value is also
// the upper bound for DATASYSTEM_SET_MEMCOPY_THREAD_NUM so an explicit setting
// cannot silently be clamped below the documented default.
static constexpr int SET_MEMCOPY_MAX_THREAD_NUM = 8;
static constexpr uint64_t SET_MEMCOPY_PARALLEL_THRESHOLD = 4 * datasystem::MB_TO_BYTES;
static constexpr int SHM_REF_RECONCILE_INTERVAL_MS = 5 * 1000;

namespace datasystem {
namespace {
constexpr size_t SET_ROUTE_MAX_ATTEMPTS = 3;
constexpr size_t STALE_LOCATION_REFRESH_ATTEMPTS = 5;
constexpr int64_t STALE_LOCATION_REFRESH_INITIAL_BACKOFF_MS = 20;
constexpr size_t DRAINING_LOCATION_REFRESH_ATTEMPTS = 3;
constexpr int64_t DRAINING_LOCATION_REFRESH_INITIAL_BACKOFF_MS = 1;
constexpr int32_t HASH_RING_RPC_MIN_TIMEOUT_MS = 100;
constexpr int BOUND_WORKER_PROBE_TIMEOUT_MS = 10;
constexpr int TRANSPORT_DIAG_LOG_RATE = 100;
const std::unordered_set<std::string> KV_CLIENT_LOG_CONFIG_KEYS = {
    "log_filename",
    "client_access_log_filename",
    "client_log_without_pid",
};
std::mutex g_kvClientConfigMutex;
bool g_hasKvClientProcessConfig = false;
std::unordered_map<std::string, std::string> g_kvClientProcessConfig;

enum class TransportReadRetryPolicy : uint8_t { NONE, DRAINING, STALE };

struct TransportReadRetryState {
    size_t outputIndex = 0;
    TransportReadRetryPolicy policy = TransportReadRetryPolicy::NONE;
    uint8_t drainingRetryCount = 0;
    uint8_t staleRetryCount = 0;
    int64_t drainingBackoffMs = DRAINING_LOCATION_REFRESH_INITIAL_BACKOFF_MS;
    int64_t staleBackoffMs = STALE_LOCATION_REFRESH_INITIAL_BACKOFF_MS;
};

struct TransportReadRoundResult {
    std::vector<std::shared_ptr<Buffer>> buffers;
    std::vector<Status> statuses;
};

TransportReadRetryPolicy ClassifyTransportReadRetry(const Status &status)
{
    if (client::IsWorkerDrainingForScaleIn(status)) {
        return TransportReadRetryPolicy::DRAINING;
    }
    if (client::IsTransportSnapshotStaleLocation(status)) {
        return TransportReadRetryPolicy::STALE;
    }
    return TransportReadRetryPolicy::NONE;
}

size_t TransportReadRetryLimit(TransportReadRetryPolicy policy)
{
    return policy == TransportReadRetryPolicy::DRAINING ? DRAINING_LOCATION_REFRESH_ATTEMPTS
                                                        : STALE_LOCATION_REFRESH_ATTEMPTS;
}

uint8_t TransportReadRetryCount(const TransportReadRetryState &state)
{
    return state.policy == TransportReadRetryPolicy::DRAINING ? state.drainingRetryCount : state.staleRetryCount;
}

uint8_t &TransportReadRetryCount(TransportReadRetryState &state)
{
    return state.policy == TransportReadRetryPolicy::DRAINING ? state.drainingRetryCount : state.staleRetryCount;
}

int64_t &TransportReadRetryBackoffMs(TransportReadRetryState &state)
{
    return state.policy == TransportReadRetryPolicy::DRAINING ? state.drainingBackoffMs : state.staleBackoffMs;
}

void UpdateTransportReadRetryState(const Status &status, TransportReadRetryState &state)
{
    state.policy = ClassifyTransportReadRetry(status);
}

void CollectInitialTransportReadRetryStates(const std::vector<Status> &itemStatuses,
                                            std::vector<TransportReadRetryState> &retryStates)
{
    for (size_t i = 0; i < itemStatuses.size(); ++i) {
        if (itemStatuses[i].IsOk()) {
            continue;
        }
        const auto policy = ClassifyTransportReadRetry(itemStatuses[i]);
        if (policy != TransportReadRetryPolicy::NONE) {
            TransportReadRetryState state;
            state.outputIndex = i;
            state.policy = policy;
            retryStates.emplace_back(std::move(state));
        }
    }
}

void CollectRetryTransportReadRound(const std::vector<size_t> &pendingStateIndexes,
                                    TransportReadRoundResult &roundResult,
                                    std::vector<std::shared_ptr<Buffer>> &buffers,
                                    std::vector<Status> &itemStatuses,
                                    std::vector<TransportReadRetryState> &retryStates)
{
    for (size_t i = 0; i < pendingStateIndexes.size(); ++i) {
        auto &state = retryStates[pendingStateIndexes[i]];
        itemStatuses[state.outputIndex] = roundResult.statuses[i];
        if (roundResult.statuses[i].IsOk()) {
            buffers[state.outputIndex] = std::move(roundResult.buffers[i]);
        }
        UpdateTransportReadRetryState(roundResult.statuses[i], state);
    }
}

bool CanRetryTransportRead(const TransportReadRetryState &state)
{
    return state.policy != TransportReadRetryPolicy::NONE &&
           TransportReadRetryCount(state) < TransportReadRetryLimit(state.policy);
}

std::vector<size_t> BuildNextTransportReadRetry(const std::vector<TransportReadRetryState> &states)
{
    TransportReadRetryPolicy policy = TransportReadRetryPolicy::NONE;
    uint8_t retryCount = std::numeric_limits<uint8_t>::max();
    std::vector<size_t> indexes;
    for (size_t i = 0; i < states.size(); ++i) {
        const auto &state = states[i];
        if (!CanRetryTransportRead(state)) {
            continue;
        }
        const auto stateRetryCount = TransportReadRetryCount(state);
        const bool higherPriority = state.policy == TransportReadRetryPolicy::DRAINING
                                    && policy != TransportReadRetryPolicy::DRAINING;
        if (policy == TransportReadRetryPolicy::NONE || higherPriority
            || (state.policy == policy && stateRetryCount < retryCount)) {
            policy = state.policy;
            retryCount = stateRetryCount;
            indexes.clear();
        }
        if (state.policy == policy && stateRetryCount == retryCount) {
            indexes.push_back(i);
        }
    }
    return indexes;
}

Status PrepareTransportReadRetry(const std::shared_ptr<client::Routing> &routing,
                                 const std::vector<size_t> &retryIndexes,
                                 std::vector<TransportReadRetryState> &states,
                                 client::DeadlineRetry &retry, bool &refreshRequested)
{
    auto &firstState = states[retryIndexes.front()];
    const bool draining = firstState.policy == TransportReadRetryPolicy::DRAINING;
    const auto retryCount = TransportReadRetryCount(firstState);
    const bool immediateStaleRetry = !draining && retryCount == 0;
    int64_t nextBackoffMs =
        client::SelectLocationRefreshBackoffMs(draining, retryCount, TransportReadRetryBackoffMs(firstState));
    LOG_EVERY_N(WARNING, TRANSPORT_DIAG_LOG_RATE)
        << "[TransportGet][Route] Retry " << (draining ? "draining" : "stale")
        << " locations, key count: " << retryIndexes.size() << ", retry count: "
        << (static_cast<int>(retryCount) + 1) << ", backoff ms: " << nextBackoffMs
        << ", remaining deadline us: " << ApiDeadline::Instance().ApiRemainingUs();
    if (!refreshRequested) {
        if (routing != nullptr) {
            routing->ForceRefresh();
        }
        refreshRequested = true;
    }
    RETURN_IF_NOT_OK(retry.Backoff(nextBackoffMs));
    for (auto index : retryIndexes) {
        ++TransportReadRetryCount(states[index]);
        if (!immediateStaleRetry) {
            TransportReadRetryBackoffMs(states[index]) = nextBackoffMs;
        }
    }
    return Status::OK();
}

void ApplyTransportReadRetryWaitFailure(const std::vector<size_t> &retryIndexes,
                                        const std::vector<TransportReadRetryState> &states,
                                        const Status &waitStatus, std::vector<Status> &itemStatuses)
{
    for (auto index : retryIndexes) {
        const auto outputIndex = states[index].outputIndex;
        Status deadlineStatus = waitStatus;
        deadlineStatus.AppendMsg(itemStatuses[outputIndex].GetMsg());
        itemStatuses[outputIndex] = std::move(deadlineStatus);
    }
}

void ApplyTransportReadRetryBudgetFailure(const std::vector<TransportReadRetryState> &states,
                                          std::vector<Status> &itemStatuses)
{
    const bool deadlineExpired = ApiDeadline::Instance().ApiRemainingUs() <= 0;
    for (const auto &state : states) {
        if (state.policy != TransportReadRetryPolicy::STALE
            || (!deadlineExpired && CanRetryTransportRead(state))) {
            continue;
        }
        const auto outputIndex = state.outputIndex;
        if (outputIndex >= itemStatuses.size()
            || !client::IsTransportSnapshotStaleLocation(itemStatuses[outputIndex])) {
            continue;
        }
        Status finalStatus = deadlineExpired
                                 ? Status(K_RPC_DEADLINE_EXCEEDED,
                                          "API deadline exceeded while waiting for transport snapshot refresh")
                                 : Status(K_RPC_UNAVAILABLE,
                                          "Transport snapshot refresh exhausted before a readable replica was found");
        finalStatus.AppendMsg(itemStatuses[outputIndex].GetMsg());
        itemStatuses[outputIndex] = std::move(finalStatus);
    }
}

void LogClientConfigInitSnapshot()
{
    DynamicFlagConfig flagConfig;
#ifdef USE_URMA
    constexpr char urmaCompiled[] = "1";
#else
    constexpr char urmaCompiled[] = "0";
#endif
    OperationLogger::Instance().LogConfigInit("URMA_COMPILED=" + std::string(urmaCompiled)
                                              + "\nGIT_COMMIT=" + GetGitHash() + "\n"
                                              + flagConfig.GetAllFlagsStr());
}

std::unordered_map<std::string, std::string> GetGflagArgs(const KVClientConfig &clientConfig)
{
    std::unordered_map<std::string, std::string> args;
    for (const auto &arg : clientConfig.GetArgs()) {
        if (KV_CLIENT_LOG_CONFIG_KEYS.find(arg.first) == KV_CLIENT_LOG_CONFIG_KEYS.end()) {
            args.emplace(arg.first, arg.second);
        }
    }
    return args;
}

void ApplyKvClientLogConfig(const KVClientConfig &clientConfig)
{
    const auto &args = clientConfig.GetArgs();
    auto logName = args.find("log_filename");
    if (logName != args.end()) {
        Logging::SetClientLogName(logName->second);
    }
    auto logWithoutPid = args.find("client_log_without_pid");
    if (logWithoutPid != args.end()) {
        Logging::SetClientLogWithoutPid(ParseBoolFromString(logWithoutPid->second, false));
    }
    auto accessLogName = args.find("client_access_log_filename");
    if (accessLogName != args.end()) {
        Logging::SetClientAccessLogName(accessLogName->second);
    }
}

Status ApplyKvClientProcessConfig(const KVClientConfig &clientConfig)
{
    std::lock_guard<std::mutex> lock(g_kvClientConfigMutex);
    if (g_hasKvClientProcessConfig) {
        for (const auto &arg : clientConfig.GetArgs()) {
            auto it = g_kvClientProcessConfig.find(arg.first);
            if (it == g_kvClientProcessConfig.end() || it->second != arg.second) {
                LOG(ERROR) << "The KVClient config [" << arg.first << "=" << arg.second
                           << "] is different from the process-level config and will not take effect.";
            }
        }
        return Status::OK();
    }

    auto gflagArgs = GetGflagArgs(clientConfig);
    if (!gflagArgs.empty()) {
        std::string errMsg;
        CHECK_FAIL_RETURN_STATUS(ParseCommandLineFlags(gflagArgs, errMsg), K_INVALID, errMsg);
    }
    ApplyKvClientLogConfig(clientConfig);
    g_kvClientProcessConfig = clientConfig.GetArgs();
    g_hasKvClientProcessConfig = true;
    return Status::OK();
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

static uint64_t CalculateDeviceBlobSize(const std::vector<DeviceBlobList> &BlobLists)
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
    return totalSize;
}

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

// A remote source group: objects sharing one remote root identity. Owns only fixed-size H2DObjectView
// entries and never copies a DeviceBlobList or Blob. Defined
// unconditionally because the grouping containers in HostDataCopy2Device are parsed on every build (the
// RH2D branch is only reachable when IsRemoteH2DEnabled() is true, but the compiler must still resolve the
// type).
struct RemoteH2DGroup {
    std::string rootInternal;
    std::vector<datasystem::object_cache::H2DObjectView> objects;
};

constexpr size_t REMOTE_H2D_GROUP_INITIAL_RESERVE = 8;

#ifdef USE_NPU
// One request-owned flat allocation backing the P2pScatterEntry array for a synchronous ScatterBatch call.
// Replaces the former per-object std::vector<std::vector<void*>> dstBufs / counts. entries[i].dstBufs/counts
// point into dstBuffers/sizes; no vector may resize after the pointers are assigned. Storage lives on the
// builder's stack and outlives the synchronous ScatterBatch call.
struct ScatterBatchStorage {
    std::vector<P2pScatterEntry> entries;
    std::vector<void *> dstBuffers;
    std::vector<uint64_t> sizes;
};
#endif

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
static_assert(static_cast<uint8_t>(datasystem::DataPlacementPolicy::PREFERRED_SAME_NODE)
                  == static_cast<uint8_t>(client::DataPlacementPolicy::PREFERRED_SAME_NODE)
              && static_cast<uint8_t>(datasystem::DataPlacementPolicy::REQUIRED_SAME_NODE)
                     == static_cast<uint8_t>(client::DataPlacementPolicy::REQUIRED_SAME_NODE)
              && static_cast<uint8_t>(datasystem::DataPlacementPolicy::PREFERRED_META_OWNER)
                     == static_cast<uint8_t>(client::DataPlacementPolicy::PREFERRED_META_OWNER),
              "Public and internal data placement policies must stay aligned");
namespace {
static constexpr int32_t INIT_SELECT_WORKER_RETRY_INTERVAL_MS = 100;
static constexpr int32_t INIT_SELECT_WORKER_NO_WORKER_RETRY_INTERVAL_MS = 500;
static constexpr int32_t INIT_SELECT_WORKER_TRIES = 6;

class RoutingExistAdapter : public IExistRouting {
public:
    explicit RoutingExistAdapter(std::shared_ptr<client::Routing> routing) : routing_(std::move(routing)) {}
    ~RoutingExistAdapter() override = default;

    Status SelectWorkers(const std::vector<std::string> &keys, client::DataPlacementPolicy policy,
                         std::unordered_map<HostPort, std::vector<std::string>> &groups,
                         const std::vector<HostPort> &exclude) override
    {
        RETURN_RUNTIME_ERROR_IF_NULL(routing_);
        return routing_->SelectWorkers(keys, policy, groups, exclude);
    }

    void UpdateState(const HostPort &addr, StatusCode status) override
    {
        if (routing_ != nullptr) {
            routing_->UpdateState(addr, status);
        }
    }

private:
    std::shared_ptr<client::Routing> routing_;
};

class TransportLayerExistAdapter : public IExistTransport {
public:
    explicit TransportLayerExistAdapter(client::TransportLayer *transport) : transport_(transport) {}
    ~TransportLayerExistAdapter() override = default;

    Status Exist(const HostPort &workerAddr, const client::TransportExistRequest &input,
                 client::TransportExistResult &output) override
    {
        RETURN_RUNTIME_ERROR_IF_NULL(transport_);
        return transport_->Exist(workerAddr, input, output);
    }

private:
    client::TransportLayer *transport_;
};

}  // namespace

ObjectClientImpl::ObjectClientImpl(const ConnectOptions &connectOptions1)
    : shmRecoveryState_(std::make_unique<ShmRecoveryState>())
{
    failover_ = std::make_unique<WorkerFailover>(*this);
    BoundMode::Deps boundDeps{ workerApi_,
                               mmapManager_,
                               &memoryRefCount_,
                               &globalRefCount_,
                               &globalRefMutex_,
                               transportLayer_,
                               routing_,
                               memoryCopyThreadPool_,
                               asyncReleasePool_,
                               asyncGetRPCPool_,
                               asyncPipelineRH2DPool_,
                               simpleIdRe_,
                               failover_.get(),
                               shutdownMux_,
                               currentNode_,
                               requestTimeoutMs_,
                               tenantId_,
                               token_,
                               enableLocalCache_,
                               enableClientDirectPipelineH2D_,
                               parallismNum_,
                               [this](std::shared_ptr<IClientWorkerApi> &workerApi, std::unique_ptr<Raii> &raii) {
                                   return GetAvailableWorkerApi(workerApi, raii);
                               },
                               [this](std::shared_ptr<IClientWorkerApi> &workerApi, std::unique_ptr<Raii> &raii,
                                      WorkerNode &workerNode) {
                                   return GetAvailableWorkerApi(workerApi, raii, workerNode);
                               } };
    boundDeps.host = BoundMode::HostServices{
        [this]() { return shared_from_this(); },
        [this]() { return IsClientReady(); },
        [this]() { return CheckConnection(); },
        [this]() { return CheckConnectionWhileShmModify(); },
        [this](uint32_t version) { return IsBufferAlive(version); },
        [this](const std::shared_ptr<ObjectBufferInfo> &bufferInfo,
               const std::unordered_set<std::string> &nestedObjectKeys, bool isSeal) {
            return PublishRoutedBuffer(bufferInfo, nestedObjectKeys, isSeal);
        },
        [this](const Status &rc, SetFailureStage failureStage, const HostPort &worker,
               std::vector<HostPort> &excludedWorkers) -> bool {
            return HandleSetRouteFailure(rc, failureStage, worker, excludedWorkers);
        },
        [this](const std::shared_ptr<IClientWorkerApi> &workerApi, const Status &status) {
            HandleDirectGetFailure(workerApi, status);
        },
        [this](const Status &rc) { return IsRoutingEvictionFailure(rc); },
        [this](HostPort &addr) { return GetCurrentWorkerHostPort(addr); },
        [this](const std::vector<std::string> &objectKeyList, const std::vector<uint64_t> &dataSizeList,
               const FullParam &param, const bool skipCheckExistence, std::vector<std::shared_ptr<Buffer>> &bufferList,
               std::vector<bool> &exists) {
            return MultiCreate(objectKeyList, dataSizeList, param, skipCheckExistence, bufferList, exists);
        },
        [this](const std::vector<std::string> &objectKeys, client::ObjectReadRequest &request,
               std::vector<Status> &itemStatuses, int64_t subTimeoutMs, bool queryL2Cache) {
            BuildTransportReadRequest(objectKeys, request, itemStatuses, subTimeoutMs, queryL2Cache);
        },
        [this](const std::vector<std::shared_ptr<Buffer>> &bufferList, const MultiPublishRspPb &rsp) -> Status {
            return HandleShmRefCountAfterMultiPublish(bufferList, rsp);
        },
        [this](const std::vector<std::string> &objectKeys, const std::vector<Blob> &devBlob,
               std::vector<std::shared_ptr<Buffer>> &buffers, void *h2dStream,
               std::vector<std::string> &failedKeys) -> Status {
#ifdef BUILD_PIPLN_H2D
            return RunClientDirectPipelineRH2D(objectKeys, devBlob, buffers, h2dStream, failedKeys);
#else
            (void)objectKeys;
            (void)devBlob;
            (void)buffers;
            (void)h2dStream;
            failedKeys.clear();
            return Status(K_NOT_SUPPORTED, "not build with BUILD_PIPLN_H2D");
#endif
        },
        [this](std::promise<AsyncResult> &promise, PiplnRh2dParam &piplnRh2dParam, GetRspPb &rsp,
               std::vector<std::shared_ptr<Buffer>> &buffers) -> Status {
#ifdef BUILD_PIPLN_H2D
            return PostPipelineRH2D(promise, piplnRh2dParam, rsp, buffers);
#else
            (void)promise;
            (void)piplnRh2dParam;
            (void)rsp;
            (void)buffers;
            return Status(K_NOT_SUPPORTED, "not build with BUILD_PIPLN_H2D");
#endif
        },
    };
    boundMode_ = std::make_unique<BoundMode>(boundDeps);
    (void)Provider::Instance();
    intern::StringPool::InitAll(false);
    clientStateManager_ = std::make_unique<ClientStateManager>();
    ConnectOptions connectOptions = connectOptions1;
    ReadOptFromEnv(connectOptions);
    ipAddress_ = HostPort(connectOptions.host, connectOptions.port);
    connectTimeoutMs_ = connectOptions.connectTimeoutMs;
    requestTimeoutMs_ = connectOptions.requestTimeoutMs != 0 ? connectOptions.requestTimeoutMs : connectTimeoutMs_;
    token_ = connectOptions.token;
    transportToken_ = std::make_shared<const SensitiveValue>(connectOptions.token);
    tenantId_ = connectOptions.tenantId;
    signature_ = std::make_unique<Signature>(connectOptions.accessKey, connectOptions.secretKey);
    enableCrossNodeConnection_ = connectOptions.enableCrossNodeConnection;
    enableLocalCache_ = connectOptions.enableLocalCache;
    dataPlacementPolicy_ = static_cast<client::DataPlacementPolicy>(connectOptions.dataPlacementPolicy);
    transportSignature_ = std::make_shared<Signature>(connectOptions.accessKey, connectOptions.secretKey);
    (void)authKeys_.SetClientPublicKey(connectOptions.clientPublicKey);
    (void)authKeys_.SetClientPrivateKey(connectOptions.clientPrivateKey);
    LOG_IF_ERROR(authKeys_.SetServerKey(WORKER_SERVER_NAME, connectOptions.serverPublicKey),
                 "RpcAuthKeys SetServerKey failed");
    enableRemoteH2D_ = connectOptions.enableRemoteH2D;
    enableClientDirectPipelineH2D_ = connectOptions.enableClientDirectPipelineH2D;
    clientDirectPipelineH2DThreadNum_ = connectOptions.clientDirectPipelineH2DThreadNum;
    serviceDiscovery_ = connectOptions.serviceDiscovery;
    fastTransportMemSize_ = connectOptions.fastTransportMemSize;
    deviceId_ = connectOptions.deviceId;
}

ObjectClientImpl::~ObjectClientImpl()
{
    auto shutdownFunc = std::bind(&ObjectClientImpl::ShutDown, this, true, true);
    clientStateManager_->ProcessDestruct(shutdownFunc);
}

void ObjectClientImpl::CleanupPreRegisteredDeviceMemory()
{
#ifdef USE_NPU
    std::vector<void *> addrs;
    {
        std::lock_guard<std::mutex> lock(preRegisteredDeviceMemoryMutex_);
        addrs.swap(preRegisteredDeviceMemoryAddrs_);
    }
    if (!addrs.empty()) {
        LOG_IF_ERROR(RemoteH2DManager::Instance().UnregisterDeviceMemory(addrs),
                     "Failed to unregister pre-registered RemoteH2D device memory");
    }
#endif
}

Status ObjectClientImpl::ShutDown(bool &needRollbackState, bool isDestruct)
{
    ShutdownMetricsThread(!isDestruct);
    ShutdownPerfThread();
    ShutdownShmRefReconcileThread();
    ShutdownPiplnMsgQueueThread();
    StopWorkerHealthProbe();
    INJECT_POINT("ObjClient.ShutDown");
    // Step0: Check client's status to determine whether it meets the conditions for executing shutdown.
    Status rc = clientStateManager_->ProcessShutdown(needRollbackState, isDestruct);
    if (!needRollbackState) {
        return rc;
    }
    // When invoked from ~ObjectClientImpl (isDestruct=true), this runs during process
    // teardown if the client is process-static (e.g. a static shared_ptr<ObjectClient>
    // in a test, or a global in an embedding host). C++ destroys thread_local objects
    // before process-static ones, so the main thread's thread_local Trace (returned by
    // Trace::Instance()) may already be destroyed here. Touching it — SetTraceUUID
    // writing traceID_/cachedHash_, and ~TraceGuard clearing them — is then a
    // heap-use-after-free (the bug exposed by TestTraceDestructorHeapUseAfterFree under
    // brpc). Skip the trace guard on the destruct path; the trace is for in-flight RPC
    // correlation and is useless once the client is being torn down anyway.
    std::optional<TraceGuard> traceGuard;
    if (!isDestruct) {
        traceGuard.emplace(Trace::Instance().SetTraceUUID());
    }

    // Stop new release submissions and drain queued reference releases while worker transports are still alive.
    auto asyncReleasePool = asyncReleasePool_;
    {
        std::lock_guard<std::shared_timed_mutex> lck(shutdownMux_);
        asyncReleasePool_ = nullptr;
    }
    asyncReleasePool = nullptr;
    auto routing = std::atomic_exchange(&routing_, std::shared_ptr<client::Routing>{});
    if (routing != nullptr) {
        routing->Shutdown();
    }
    if (transportLayer_ != nullptr) {
        transportLayer_->Shutdown();
        transportLayer_.reset();
    }
    asyncSetRPCPool_ = nullptr;
    asyncGetRPCPool_ = nullptr;
    asyncPipelineRH2DPool_ = nullptr;
    asyncGetCopyPool_ = nullptr;
    asyncDevDeletePool_ = nullptr;

    if (devOcImpl_ != nullptr) {
        devOcImpl_->SetThreadInterruptFlag2True();
    }
    CleanupPreRegisteredDeviceMemory();

    // Step0: notify wait post.
    switchPost_.Set();
    // Step1: Shutdown heartbeat.
    for (size_t i = 0; i < listenWorker_.size(); i++) {
        if (listenWorker_[i] != nullptr) {
            listenWorker_[i]->StopListenWorker(true);
        }
    }
    // Clear URMA data-plane failure callbacks before draining: each captures WorkerFailover*
    // (failover_, destructed before workerApi_), so leaving it installed risks a dangling
    // dereference between failover_ destruction and workerApi_ destruction.
    for (const auto &api : workerApi_) {
        if (api != nullptr) {
            api->SetUrmaDataPlaneFailureCallback(nullptr);
        }
    }
    failover_->DrainAsyncSwitchWorkerPool();
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
        RETURN_IF_NOT_OK(signature_->SetClientAkSk(args.at("system_access_key"), args.at("system_secret_key")));
        RETURN_RUNTIME_ERROR_IF_NULL(transportSignature_);
        RETURN_IF_NOT_OK(
            transportSignature_->SetClientAkSk(args.at("system_access_key"), args.at("system_secret_key")));
    }
    if (args.find("connectTimeoutMs") != args.end()) {
        int result = 0;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Uri::StrToInt(args.at("connectTimeoutMs").c_str(), result),
                                             K_RUNTIME_ERROR, "connectTimeoutMs to int failed");
        connectTimeoutMs_ = result;
    }
    if (args.find("requestTimeoutMs") != args.end()) {
        int result = 0;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Uri::StrToInt(args.at("requestTimeoutMs").c_str(), result),
                                             K_RUNTIME_ERROR, "requestTimeoutMs to int failed");
        requestTimeoutMs_ = result;
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
    LogClientConfigInitSnapshot();
    return Status::OK();
}

void ObjectClientImpl::ConstructTreadPool()
{
    const size_t threadCount = 8;
    asyncSetRPCPool_ = std::make_shared<ThreadPool>(0, threadCount, "async_set");
    asyncGetCopyPool_ = std::make_shared<ThreadPool>(0, threadCount, "async_get_copy");
    asyncGetRPCPool_ = std::make_shared<ThreadPool>(0, threadCount, "async_get_rpc");
    asyncPipelineRH2DPool_ = std::make_shared<ThreadPool>(0, threadCount, "async_pipeline_rh2d");
    asyncSwitchWorkerPool_ = std::make_shared<ThreadPool>(0, 1, "switch");
    asyncSwitchWorkerPoolHandle_ = asyncSwitchWorkerPool_.get();
    asyncDevDeletePool_ = std::make_shared<ThreadPool>(0, threadCount);
    asyncReleasePool_ = std::make_shared<ThreadPool>(0, 4, "async_release_buffer");
}

Status ObjectClientImpl::InitTransportLayer()
{
    if (transportLayer_ != nullptr) {
        return Status::OK();
    }
    RETURN_RUNTIME_ERROR_IF_NULL(transportSignature_);
    RETURN_RUNTIME_ERROR_IF_NULL(asyncGetRPCPool_);
    RETURN_RUNTIME_ERROR_IF_NULL(asyncReleasePool_);
    if (ubHealthFilter_ == nullptr) {
        ubHealthFilter_ = std::make_shared<client::UbHealthFilter>();
    }
    BrpcChannelConfig channelConfig;
    channelConfig.timeout_ms = requestTimeoutMs_;
    channelConfig.connect_timeout_ms = connectTimeoutMs_;
    client::TransportLayerOptions options;
    options.channelConfig = std::move(channelConfig);
    options.releasePool = asyncReleasePool_;
    options.enableClientDirectPipelineH2D = enableClientDirectPipelineH2D_;
    options.pipelineThreadNum = clientDirectPipelineH2DThreadNum_;
    // RegisterClient decides whether this process can actually use UB and requests the runtime before this method.
    // Do not infer UB capability from local-cache settings: doing so makes TCP/SHM-only clients scan UB devices and
    // allocate/register the transport memory pool even when no worker has advertised UB support.
    options.initializeUbRuntime = IsUrmaRuntimeConfigured();
    // UB prewarming is optional while the bound endpoint has a working SHM path. Keep that path available when the
    // local client cannot initialize UB; a non-SHM endpoint retains the existing fail-fast behavior.
    options.allowUbRuntimeFailure = workerApi_[currentNode_]->IsShmEnable();
    options.readSourceFilter = ubHealthFilter_;
    options.metadataFailureHandler = [this](const HostPort &owner, const Status &status) {
        if (!ShouldRefreshRoutingAfterFailure(status.GetCode())
            && !client::IsTransportSnapshotStaleLocation(status)) {
            return;
        }
        auto routing = std::atomic_load(&routing_);
        if (routing != nullptr && routing->ForceRefresh()) {
            LOG(INFO) << "[Routing] Force hash ring refresh after metadata owner access failure, metadata owner: "
                      << owner.ToString() << ", status: " << status.ToString();
        }
    };
    options.drainingFallbackHandler = [this](const HostPort &worker, const Status &status) {
        auto routing = std::atomic_load(&routing_);
        if (routing != nullptr && routing->ForceRefresh()) {
            LOG(INFO) << "[Routing] Force hash ring refresh after draining SHM fallback, worker: "
                      << worker.ToString() << ", status: " << status.ToString();
        }
    };
    auto transportLayer = std::make_unique<client::TransportLayer>(
        transportSignature_, asyncGetRPCPool_, fastTransportMemSize_, std::move(options));
    RETURN_IF_NOT_OK(transportLayer->Init());
    transportLayer_ = std::move(transportLayer);
    LOG(INFO) << "Client transport layer initialized";
    return Status::OK();
}

Status ObjectClientImpl::ApplyRoutingWorkerSnapshot(uint64_t ringVersion,
                                                    const ::datasystem::ClusterTopologyPb &ring,
                                                    const std::unordered_map<std::string, std::string> &hostIdMap,
                                                    const std::string &sdkHostId)
{
    RETURN_RUNTIME_ERROR_IF_NULL(transportLayer_);
    RETURN_RUNTIME_ERROR_IF_NULL(ubHealthFilter_);
    client::WorkerSnapshot snapshot;
    RETURN_IF_NOT_OK(client::BuildWorkerSnapshot(ringVersion, ring, hostIdMap, sdkHostId, snapshot));
    ubHealthFilter_->ApplyTopologyIncarnations(ring);
    RETURN_IF_NOT_OK(transportLayer_->ApplyWorkerSnapshot(std::move(snapshot)));
    MaybeSwitchWorkerRemovedFromRing(ring);
    return Status::OK();
}

Status ObjectClientImpl::InitDataPlacementPolicy()
{
    const char *policyName = nullptr;
    switch (dataPlacementPolicy_) {
        case client::DataPlacementPolicy::PREFERRED_SAME_NODE:
            policyName = "PREFERRED_SAME_NODE";
            break;
        case client::DataPlacementPolicy::REQUIRED_SAME_NODE:
            policyName = "REQUIRED_SAME_NODE";
            break;
        case client::DataPlacementPolicy::PREFERRED_META_OWNER:
            policyName = "PREFERRED_META_OWNER";
            break;
        default:
            RETURN_STATUS(K_INVALID, "Invalid data placement policy in ConnectOptions");
    }
    LOG(INFO) << "Data placement policy initialized: " << policyName;
    return Status::OK();
}

Status ObjectClientImpl::InitRouting(const HostPort &initialWorker, bool initialWorkerIsLocal)
{
    if (std::atomic_load(&routing_) != nullptr) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(!initialWorker.Empty(), K_NOT_READY,
                             "Initial worker address is unavailable for routing initialization");
    RETURN_IF_NOT_OK(InitDataPlacementPolicy());
    if (ubHealthFilter_ == nullptr) {
        ubHealthFilter_ = std::make_shared<client::UbHealthFilter>();
    }
    RETURN_RUNTIME_ERROR_IF_NULL(transportSignature_);
    BrpcChannelConfig channelConfig;
    channelConfig.timeout_ms =
        requestTimeoutMs_ <= 0 ? requestTimeoutMs_ : std::max(requestTimeoutMs_, HASH_RING_RPC_MIN_TIMEOUT_MS);
    channelConfig.connect_timeout_ms = connectTimeoutMs_;
    channelConfig.max_retry = 0;
    channelConfig.enable_circuit_breaker = false;
    // Service discovery owns SDK host-id resolution. Keep the initial-worker lookup only for
    // direct local connections that do not have a discovery object.
    auto sdkHostIdCache =
        std::make_shared<std::string>(serviceDiscovery_ == nullptr ? "" : serviceDiscovery_->GetHostId());
    auto hostIdUnresolvedWarned = std::make_shared<bool>(false);
    auto ringUpdateHook = [this, initialWorker, initialWorkerIsLocal, sdkHostIdCache, hostIdUnresolvedWarned](
                              uint64_t ringVersion, const ::datasystem::ClusterTopologyPb &ring,
                              const std::unordered_map<std::string, std::string> &hostIdMap) {
        if (sdkHostIdCache->empty()) {
            // initialWorkerIsLocal must reflect the real locality of the bound worker (threaded from
            // service-discovery selection), not a hardcoded default. When it is false, a cross-node
            // bound worker's hostId is NOT adopted, so cross-node workers use remote transport and
            // GetTransportHint selects UB/TCP instead of timing out on the SHM/UDS path.
            const auto resolved = client::ResolveSdkHostId(initialWorkerIsLocal, initialWorker, hostIdMap);
            if (!resolved.empty()) {
                *sdkHostIdCache = resolved;
            } else if (!*hostIdUnresolvedWarned) {
                *hostIdUnresolvedWarned = true;
                LOG(WARNING) << "[Routing] SDK host_id is unresolved and the bound worker is not confirmed"
                             << " same-host (initialWorker=" << initialWorker.ToString()
                             << "); same-host SHM partitioning is disabled and cross-node workers route"
                             << " via UB/TCP. Set host_id_env_name on the client process to enable SHM.";
            }
        }
        if (transportLayer_ != nullptr) {
            return ApplyRoutingWorkerSnapshot(ringVersion, ring, hostIdMap, *sdkHostIdCache);
        }
        ubHealthFilter_->ApplyTopologyIncarnations(ring);
        return Status::OK();
    };
    auto routing = std::make_shared<client::Routing>(
        std::move(channelConfig), transportSignature_, std::move(ringUpdateHook),
        std::vector<std::shared_ptr<client::IWorkerFilter>>{ ubHealthFilter_ });
    RETURN_IF_NOT_OK(routing->Init(*sdkHostIdCache, initialWorker, initialWorkerIsLocal));
    std::atomic_store(&routing_, std::move(routing));
    LOG(INFO) << "[Routing] Object client routing initialized from worker " << initialWorker.ToString();
    return Status::OK();
}

Status ObjectClientImpl::InitClientWorkerConnect(bool enableHeartbeat, bool initWithWorker,
                                                 int32_t connectTimeoutMs, bool routedWorkerIsLocal)
{
    int32_t timeoutMs = connectTimeoutMs >= 0 ? connectTimeoutMs : connectTimeoutMs_;
    CHECK_FAIL_RETURN_STATUS(timeoutMs >= 0, K_INVALID, "The connection timeout must be a positive integer.");
    RETURN_IF_NOT_OK(InitClientWorkerConnectAt(LOCAL_WORKER, ipAddress_, enableHeartbeat, initWithWorker, timeoutMs));
    // isLocalWorker stays true so InitListenWorkerAt recovery wiring is unchanged; routedWorkerIsLocal
    // carries the real locality of the bound worker so InitRouting does not adopt a cross-node bound
    // worker's hostId (which would misclassify that whole remote host as same-host and time out Gets
    // on the SHM/UDS path).
    return InitClientRuntimeAt(LOCAL_WORKER, initWithWorker, true, routedWorkerIsLocal);
}

Status ObjectClientImpl::InitClientWorkerConnectAt(WorkerNode node, const HostPort &address, bool enableHeartbeat,
                                                   bool initWithWorker, int32_t connectTimeoutMs)
{
    HeartbeatType heartbeatType = enableHeartbeat ? HeartbeatType::RPC_HEARTBEAT : HeartbeatType::NO_HEARTBEAT;
    workerApi_.resize(STANDBY2_WORKER + 1);
    if (!initWithWorker) {
        workerApi_[node] =
            std::make_shared<ClientWorkerRemoteApi>(address, heartbeatType, token_, signature_.get(), tenantId_,
                                                    enableCrossNodeConnection_, deviceId_);
    } else {
        workerApi_[node] = std::make_shared<ClientWorkerLocalApi>(address, embeddedClientWorkerApi_, worker_,
                                                                  heartbeatType, signature_.get(), false, deviceId_);
    }
    // Local-cache-off clients route every object operation through the transport layer even when worker failover is
    // disabled. Keep this intent separate from enableCrossNodeConnection_, whose registration/switch semantics must
    // not change merely to prepare UB for routed data.
    workerApi_[node]->SetMayAccessNonBoundWorker(
        ClientMayAccessNonBoundWorker(enableLocalCache_, enableCrossNodeConnection_));
    workerApi_[node]->isUseStandbyWorker_ = node != LOCAL_WORKER;
    int32_t initAttemptTimeoutMs = connectTimeoutMs == connectTimeoutMs_ ? -1 : connectTimeoutMs;
    RETURN_IF_NOT_OK(workerApi_[node]->Init(requestTimeoutMs_, connectTimeoutMs_, fastTransportMemSize_,
                                            initAttemptTimeoutMs));
    failover_->ConfigureUrmaDataPlaneFailureCallback(node, workerApi_[node]);
    return Status::OK();
}

bool ObjectClientImpl::ShouldRefreshRoutingAfterFailure(StatusCode code)
{
    return code == K_RPC_UNAVAILABLE || code == K_RPC_DEADLINE_EXCEEDED || code == K_RPC_PEER_DEAD
           || code == K_CLIENT_WORKER_DISCONNECT || code == K_METADATA_OWNER_UNAVAILABLE;
}

void ObjectClientImpl::HandleDirectGetFailure(const std::shared_ptr<IClientWorkerApi> &workerApi,
                                              const Status &status)
{
    if (!ShouldRefreshRoutingAfterFailure(status.GetCode())) {
        return;
    }
    auto routing = std::atomic_load(&routing_);
    if (routing != nullptr && routing->ForceRefresh()) {
        LOG(INFO) << "[Routing] Force hash ring refresh after direct Get failure, worker: "
                  << workerApi->hostPort_.ToString() << ", status: " << status.ToString();
    }
    if (status.GetCode() == K_RPC_PEER_DEAD) {
        (void)failover_->SubmitUnavailableWorkerSwitch(workerApi);
    }
}

void ObjectClientImpl::MaybeSwitchWorkerRemovedFromRing(const ::datasystem::ClusterTopologyPb &ring)
{
    if (!enableLocalCache_ || std::atomic_load(&routing_) == nullptr) {
        return;
    }
    std::shared_ptr<IClientWorkerApi> workerApi;
    {
        std::lock_guard<std::mutex> lock(switchNodeMutex_);
        if (static_cast<size_t>(currentNode_) >= workerApi_.size() || workerApi_[currentNode_] == nullptr) {
            return;
        }
        workerApi = workerApi_[currentNode_];
    }
    auto member = ring.members().find(workerApi->hostPort_.ToString());
    if (member != ring.members().end() && member->second.state() == ::datasystem::MembershipPb::ACTIVE) {
        return;
    }
    (void)failover_->SubmitUnavailableWorkerSwitch(workerApi);
}

Status ObjectClientImpl::InitClientRuntimeAt(WorkerNode node, bool initWithWorker, bool isLocalWorker,
                                             bool routedWorkerIsLocal)
{
    auto &workerApi = workerApi_[node];
    mmapManager_ = std::make_unique<client::MmapManager>(workerApi, initWithWorker);
    ConstructTreadPool();
    if (!enableLocalCache_ || enableClientDirectPipelineH2D_) {
        RETURN_IF_NOT_OK(InitTransportLayer());
    }

    RETURN_IF_NOT_OK(workerApi->PrepareForDecreaseShmRef(std::bind(
        &client::MmapManager::LookupUnitsAndMmapFd, mmapManager_.get(), std::placeholders::_1, std::placeholders::_2)));
    RETURN_IF_NOT_OK(workerApi->InitPipelineRH2DQueue([this](std::shared_ptr<ShmUnitInfo> &shmUnitInfo) {
        return mmapManager_->LookupUnitsAndMmapFd("", shmUnitInfo);
    }));
    clientEnableP2Ptransfer_ = workerApi->workerEnableP2Ptransfer_;
    RETURN_IF_NOT_OK(InitListenWorkerAt(node, isLocalWorker));
    RETURN_IF_NOT_OK(workerApi->TryFastTransportAfterHeartbeat());
    if (enableLocalCache_ && (IsUrmaEnabled() || enableCrossNodeConnection_)) {
        RETURN_IF_NOT_OK(InitTransportLayer());
        client::WorkerSnapshot snapshot;
        if (workerApi->IsShmEnable()) {
            snapshot.shmCandidateAddrs.emplace_back(workerApi->hostPort_);
        } else {
            snapshot.remoteTransportAddrs.emplace_back(workerApi->hostPort_);
        }
        snapshot.writeProbeAddrs.emplace_back(workerApi->hostPort_);
        RETURN_IF_NOT_OK(transportLayer_->ApplyWorkerSnapshot(std::move(snapshot)));
    }
    const bool needsRouting = !enableLocalCache_ || enableCrossNodeConnection_;
    if (needsRouting) {
        RETURN_IF_NOT_OK(InitRouting(workerApi->hostPort_, routedWorkerIsLocal));
    }
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
        std::make_shared<client::ListenWorker>(workerApi_[node], heartbeatType, node, asyncSwitchWorkerPoolHandle_);
    if (isLocalWorker) {
        listenWorker_[node]->AddRecoveryCallback(
            this, [this](client::WorkerRecoveryReason reason) { return failover_->ProcessWorkerLost(reason); });
        listenWorker_[node]->SetWorkerTimeoutHandle([this] { failover_->ProcessWorkerTimeout(); });
        listenWorker_[node]->SetReleaseFdCallBack(
            [this](const std::vector<int64_t> &fds) { mmapManager_->ClearExpiredFds(fds); });
    } else {
        listenWorker_[node]->AddRecoveryCallback(
            this,
            [this, node](client::WorkerRecoveryReason reason) {
            return failover_->ProcessStandbyWorkerLost(node, reason);
        });
        if (serviceDiscovery_ != nullptr && serviceDiscovery_->HasHostAffinity()) {
            listenWorker_[node]->SetRecoverLocalWorkerHandle(
                [this]() { return failover_->RecoverPreferredLocalWorker(); });
        }
    }
    if (enableCrossNodeConnection_) {
        listenWorker_[node]->SetSwitchWorkerHandle([this](uint32_t index, client::SwitchTriggerReason reason) {
            return failover_->SwitchWorkerNode(static_cast<WorkerNode>(index), reason);
        });
    }
    listenWorker_[node]->SetIsLocalWorker(isLocalWorker);
    RETURN_IF_NOT_OK(listenWorker_[node]->StartListenWorker());
    return Status::OK();
}

Status ObjectClientImpl::InitPreferredRemoteFallback(const HostPort &remoteAddress, bool enableHeartbeat,
                                                     int32_t connectTimeoutMs)
{
    CHECK_FAIL_RETURN_STATUS(connectTimeoutMs >= 0, K_INVALID, "The connection timeout must be a positive integer.");
    RETURN_IF_NOT_OK(InitClientWorkerConnectAt(STANDBY1_WORKER, remoteAddress, enableHeartbeat, false,
                                               connectTimeoutMs));
    currentNode_ = STANDBY1_WORKER;
    RETURN_IF_NOT_OK(InitClientRuntimeAt(STANDBY1_WORKER, false, false, false));
    LOG(INFO) << "[Switch] Preferred same-node local worker is absent, use remote fallback "
              << remoteAddress.ToString();
    return Status::OK();
}

bool ObjectClientImpl::ShouldRetryInit(const Status &status) const
{
    if (IsRetryableRpcError(status)) {
        return true;
    }
    switch (status.GetCode()) {
        case K_CLIENT_WORKER_DISCONNECT:
        case K_TRY_AGAIN:
            return true;
        // During init, a peer-dead (ECONNREFUSED/ENOTCONN/EHOSTDOWN from a worker that is
        // mid-restart) must be retried within the connect budget — the peer is booting, not
        // gone. Master treated these as K_RPC_UNAVAILABLE and retried; this PR's remap to
        // K_RPC_PEER_DEAD would otherwise abort init on the first refused connection. The
        // runtime fast-fail contract (dead_peer_fast_fail_test) covers steady-state RPCs,
        // not init, so retrying here does not weaken it.
        case K_RPC_PEER_DEAD:
            return true;
        default:
            return false;
    }
}

void ObjectClientImpl::ClearFailedInitAttempt()
{
    ShutdownMetricsThread(false);
    ShutdownPerfThread();
    ShutdownShmRefReconcileThread();
    asyncReleasePool_ = nullptr;
    auto routing = std::atomic_exchange(&routing_, std::shared_ptr<client::Routing>{});
    if (routing != nullptr) {
        routing->Shutdown();
    }
    if (transportLayer_ != nullptr) {
        transportLayer_->Shutdown();
        transportLayer_.reset();
    }
    for (auto &listener : listenWorker_) {
        if (listener != nullptr) {
            listener->StopListenWorker(true);
        }
    }
    failover_->DrainAsyncSwitchWorkerPool();
    for (auto &api : workerApi_) {
        if (api != nullptr) {
            LOG_IF_ERROR(api->Disconnect(false), "Disconnect failed init worker.");
        }
    }
    listenWorker_.clear();
    workerApi_.clear();
    currentNode_ = LOCAL_WORKER;
    switchInProgress_ = false;
    workerSwitchState_ = WorkerSwitchState::AVAILABLE;
    mmapManager_.reset();
    devOcImpl_.reset();
    asyncSetRPCPool_ = nullptr;
    asyncGetRPCPool_ = nullptr;
    asyncPipelineRH2DPool_ = nullptr;
    asyncGetCopyPool_ = nullptr;
    asyncDevDeletePool_ = nullptr;
}

Status ObjectClientImpl::Init(bool &needRollbackState, bool enableHeartbeat, const KVClientConfig *clientConfig)
{
    if (clientConfig != nullptr) {
        RETURN_IF_NOT_OK(ApplyKvClientProcessConfig(*clientConfig));
    }
    Logging::GetInstance()->Start(CLIENT_LOG_FILENAME, LogProcessRole::CLIENT);
    FlagsMonitor::GetInstance()->Start();
    LOG_IF_ERROR(PreExpandFdPool(FLAGS_fd_pool_prewarm_size), "Failed to pre-expand fd pool.");

    auto rc = clientStateManager_->ProcessInit(needRollbackState);
    if (!needRollbackState) {
        return rc;
    }

    if (serviceDiscovery_ != nullptr) {
        return InitWithServiceDiscovery(enableHeartbeat);
    }

    return InitWorkerClientAtCurrentAddress(enableHeartbeat, true);
}

Status ObjectClientImpl::InitWorkerClientAtCurrentAddress(bool enableHeartbeat, bool isSameNode,
                                                          int32_t connectTimeoutMs)
{
    std::string hostPortStr = ipAddress_.ToString();
    if (hostPortStr.empty()) {
        return Status(K_INVALID, "ConnectOptions was not configured with a host and port or serviceDiscovery.");
    }

    CHECK_FAIL_RETURN_STATUS(
        Validator::ValidateHostPortString("HostPort", hostPortStr), K_INVALID,
        FormatString("Invalid IP address/port. Host %s, port: %d", ipAddress_.Host(), ipAddress_.Port()));

    LOG(INFO) << "Start to init worker client at address: " << hostPortStr;

    Status rc;
    if (!isSameNode && serviceDiscovery_ != nullptr && serviceDiscovery_->HasHostAffinity()) {
        LOG(INFO) << "Start to init preferred remote fallback worker client at address: " << hostPortStr;
        rc = InitPreferredRemoteFallback(ipAddress_, enableHeartbeat, connectTimeoutMs);
    } else {
        rc = InitClientWorkerConnect(enableHeartbeat, false, connectTimeoutMs, isSameNode);
    }
    RETURN_IF_NOT_OK(rc);
    LogClientConfigInitSnapshot();
    return Status::OK();
}

Status ObjectClientImpl::GetCurrentWorkerHostPort(HostPort &addr) const
{
    std::lock_guard<std::mutex> lock(switchNodeMutex_);
    auto workerApi = workerApi_[currentNode_];
    CHECK_FAIL_RETURN_STATUS(workerApi != nullptr, K_NOT_READY, "Current worker API is not initialized");
    addr = workerApi->hostPort_;
    return Status::OK();
}

Status ObjectClientImpl::PickFallbackWorker(const std::unordered_set<HostPort> &failedWorkerAddrs,
                                            HostPort &outAddr, bool &outIsSameNode)
{
    std::vector<std::string> sameHostStrs;
    std::vector<std::string> otherStrs;
    Status getAllRc = serviceDiscovery_->GetAllWorkers(sameHostStrs, otherStrs);
    if (getAllRc.IsError()) {
        LOG(WARNING) << "[Init] GetAllWorkers failed during fallback: " << getAllRc.ToString()
                     << ". Retrying within Init budget.";
        return Status(K_TRY_AGAIN, "GetAllWorkers failed during fallback");
    }
    std::vector<HostPort> sameHost;
    std::vector<HostPort> others;
    auto collect = [&failedWorkerAddrs](const std::vector<std::string> &addrs, std::vector<HostPort> &out) {
        for (const auto &a : addrs) {
            HostPort hp;
            if (hp.ParseString(a).IsError()) {
                continue;
            }
            if (failedWorkerAddrs.count(hp) > 0) {
                continue;
            }
            out.emplace_back(std::move(hp));
        }
    };
    collect(sameHostStrs, sameHost);
    collect(otherStrs, others);
    ShuffleWorkerCandidates(sameHost);
    ShuffleWorkerCandidates(others);
    if (!sameHost.empty()) {
        outAddr = std::move(sameHost.front());
        outIsSameNode = true;
        return Status::OK();
    }
    if (serviceDiscovery_->GetAffinityPolicy() != ServiceAffinityPolicy::REQUIRED_SAME_NODE && !others.empty()) {
        outAddr = std::move(others.front());
        outIsSameNode = false;
        return Status::OK();
    }
    return Status(K_TRY_AGAIN, "no admissible candidate after exclusion");
}

bool ObjectClientImpl::ShouldExcludeFailedWorker(const Status &rc) const
{
    return rc.GetCode() == K_RPC_UNAVAILABLE || rc.GetCode() == K_CLIENT_WORKER_DISCONNECT;
}

Status ObjectClientImpl::SelectNextInitWorker(std::unordered_set<HostPort> &failedWorkerAddrs, HostPort &outAddr,
                                              bool &outIsSameNode, bool &outIsNoAvailableWorker)
{
    std::string workerIp;
    int workerPort;
    bool isSameNode = false;
    Status selectRc = serviceDiscovery_->SelectWorker(workerIp, workerPort, &isSameNode, &outIsNoAvailableWorker);
    if (selectRc.GetCode() == K_TRY_AGAIN) {
        return selectRc;
    }
    RETURN_IF_NOT_OK(selectRc);
    HostPort selectedAddr(workerIp, workerPort);
    if (failedWorkerAddrs.count(selectedAddr) == 0) {
        outAddr = std::move(selectedAddr);
        outIsSameNode = isSameNode;
        return Status::OK();
    }
    HostPort fallback;
    bool fallbackSameNode = false;
    Status fbRc = PickFallbackWorker(failedWorkerAddrs, fallback, fallbackSameNode);
    if (fbRc.GetCode() == K_TRY_AGAIN) {
        return fbRc;
    }
    RETURN_IF_NOT_OK(fbRc);
    LOG(INFO) << FormatString("[Init] SD-selected worker %s failed earlier this Init; "
                              "falling back to %s (isSameNode=%d)",
                              selectedAddr.ToString(), fallback.ToString(), fallbackSameNode);
    outAddr = std::move(fallback);
    outIsSameNode = fallbackSameNode;
    return Status::OK();
}

Status ObjectClientImpl::InitWithServiceDiscovery(bool enableHeartbeat)
{
    CHECK_FAIL_RETURN_STATUS(connectTimeoutMs_ >= 0, K_INVALID, "The connection timeout must be a positive integer.");
    CHECK_FAIL_RETURN_STATUS(
        connectTimeoutMs_ >= RPC_MINIMUM_TIMEOUT, K_INVALID,
        FormatString("The connectTimeoutMs(%d) should be greater than or equal to %d milliseconds.", connectTimeoutMs_,
                     RPC_MINIMUM_TIMEOUT));
    Timer timer(connectTimeoutMs_);
    int32_t remainTimeMs = static_cast<int32_t>(timer.GetRemainingTimeMs());
    int32_t retryTimes = 0;
    // Per-Init lifecycle local exclusion set: a worker that failed Init in an earlier
    // retry round is excluded for the remainder of THIS Init so the retry loop switches
    // to a different candidate instead of re-selecting the same dead worker (which
    // PREFERRED_SAME_NODE would otherwise do when the killed same-node worker is still
    // READY in etcd within the lease window). The set is a local variable, not a member,
    // so it is automatically cleared when Init() returns — a worker excluded in one
    // Init() call is fully selectable in the next, and a worker that restarts later
    // is never permanently blacklisted.
    std::unordered_set<HostPort> failedWorkerAddrs;
    auto prepareNextRetry = [&](int32_t intervalMs) -> Status {
        CHECK_FAIL_RETURN_STATUS(++retryTimes < INIT_SELECT_WORKER_TRIES, K_RPC_DEADLINE_EXCEEDED, "Rpc timeout");
        remainTimeMs = static_cast<int32_t>(timer.GetRemainingTimeMs());
        CHECK_FAIL_RETURN_STATUS(remainTimeMs > 0, K_RPC_DEADLINE_EXCEEDED, "Rpc timeout");
        std::this_thread::sleep_for(std::chrono::milliseconds(std::min(remainTimeMs, intervalMs)));
        remainTimeMs = static_cast<int32_t>(timer.GetRemainingTimeMs());
        return Status::OK();
    };
    while (remainTimeMs > 0) {
        HostPort selectedAddr;
        bool isSameNode = false;
        bool isNoAvailableWorker = false;
        Status selectRc = SelectNextInitWorker(failedWorkerAddrs, selectedAddr, isSameNode, isNoAvailableWorker);
        if (selectRc.GetCode() == K_TRY_AGAIN) {
            auto retryRc = prepareNextRetry(isNoAvailableWorker ? INIT_SELECT_WORKER_NO_WORKER_RETRY_INTERVAL_MS
                                                                : INIT_SELECT_WORKER_RETRY_INTERVAL_MS);
            if (isNoAvailableWorker && (retryRc.IsError() || remainTimeMs <= 0)) {
                return Status(K_NOT_READY, "No available worker is detected.");
            }
            RETURN_IF_NOT_OK(retryRc);
            continue;
        }
        RETURN_IF_NOT_OK(selectRc);
        ipAddress_ = selectedAddr;

        remainTimeMs = static_cast<int32_t>(timer.GetRemainingTimeMs());
        CHECK_FAIL_RETURN_STATUS(remainTimeMs >= RPC_MINIMUM_TIMEOUT, K_RPC_DEADLINE_EXCEEDED, "Rpc timeout");
        int32_t singleInitTimeoutMs = CalculateConnectAttemptTimeoutMs(connectTimeoutMs_);
        int32_t initTimeoutMs = std::min(remainTimeMs, singleInitTimeoutMs);
        Status rc = InitWorkerClientAtCurrentAddress(enableHeartbeat, isSameNode, initTimeoutMs);
        
        RETURN_OK_IF_TRUE(rc.IsOk());

        ClearFailedInitAttempt();
        if (!ShouldRetryInit(rc)) {
            return rc;
        }
        if (ShouldExcludeFailedWorker(rc)) {
            failedWorkerAddrs.insert(ipAddress_);
        }
        RETURN_IF_NOT_OK(prepareNextRetry(INIT_SELECT_WORKER_RETRY_INTERVAL_MS));
    }
    return Status(K_RPC_DEADLINE_EXCEEDED, "Rpc timeout");
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

    int setMemoryCopyThreadNum = getEnvInt(DATASYSTEM_SET_MEMCOPY_THREAD_NUM_ENV, SET_MEMCOPY_MAX_THREAD_NUM);
    if (setMemoryCopyThreadNum < 0) {
        LOG(WARNING) << FormatString("Invalid %s value: %d, using default %d", DATASYSTEM_SET_MEMCOPY_THREAD_NUM_ENV,
                                     setMemoryCopyThreadNum, SET_MEMCOPY_MAX_THREAD_NUM);
        setMemoryCopyThreadNum = SET_MEMCOPY_MAX_THREAD_NUM;
    }
    if (setMemoryCopyThreadNum > SET_MEMCOPY_MAX_THREAD_NUM) {
        LOG(WARNING) << FormatString("%s is %d, clamped to %d", DATASYSTEM_SET_MEMCOPY_THREAD_NUM_ENV,
                                     setMemoryCopyThreadNum, SET_MEMCOPY_MAX_THREAD_NUM);
        setMemoryCopyThreadNum = SET_MEMCOPY_MAX_THREAD_NUM;
    }
    if (setMemoryCopyThreadNum > 1) {
        setMemoryCopyThreadPool_ =
            std::make_shared<ThreadPool>(0, setMemoryCopyThreadNum, "set_memcopy");
    }
    setMemcpyParallelThreshold_ =
        getEnvInt(DATASYSTEM_SET_MEMCOPY_PARALLEL_THRESHOLD_ENV, SET_MEMCOPY_PARALLEL_THRESHOLD);
    LOG(INFO) << FormatString("Init Set memory copy with threadNum: %d, parallelThreshold: %lu",
                              setMemoryCopyThreadNum, setMemcpyParallelThreshold_);

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

Status ObjectClientImpl::GetAvailableWorkerApi(std::shared_ptr<IClientWorkerApi> &workerApi)
{
    std::shared_ptr<client::ListenWorker> listenWorker;
    {
        std::lock_guard<std::mutex> lock(switchNodeMutex_);
        if (workerSwitchState_ == WorkerSwitchState::NO_SWITCHABLE_WORKER) {
            return failover_->NoSwitchableWorkerStatus();
        }
        WorkerNode id = currentNode_;
        workerApi = workerApi_[id];
        if (workerApi == nullptr) {
            id = LOCAL_WORKER;
            workerApi = workerApi_[id];
        }
        listenWorker = id < listenWorker_.size() ? listenWorker_[id] : nullptr;
    }
    return CheckConnection(listenWorker, workerApi);
}

Status ObjectClientImpl::GetAvailableWorkerApi(std::shared_ptr<IClientWorkerApi> &workerApi,
                                               std::unique_ptr<Raii> &raii)
{
    WorkerNode workerNode;
    return GetAvailableWorkerApi(workerApi, raii, workerNode);
}

Status ObjectClientImpl::GetAvailableWorkerApi(std::shared_ptr<IClientWorkerApi> &workerApi,
                                               std::unique_ptr<Raii> &raii, WorkerNode &workerNode)
{
    std::shared_ptr<client::ListenWorker> listenWorker;
    {
        std::lock_guard<std::mutex> lock(switchNodeMutex_);
        if (workerSwitchState_ == WorkerSwitchState::NO_SWITCHABLE_WORKER) {
            return failover_->NoSwitchableWorkerStatus();
        }
        WorkerNode id = currentNode_;
        workerApi = workerApi_[id];
        if (workerApi == nullptr) {
            id = LOCAL_WORKER;
            workerApi = workerApi_[id];
        }
        workerNode = id;
        listenWorker = id < listenWorker_.size() ? listenWorker_[id] : nullptr;
        CHECK_FAIL_RETURN_STATUS(workerApi != nullptr, K_NOT_READY, "No available client identity");
        workerApi->IncreaseInvokeCount();
        raii = std::make_unique<Raii>([workerApi]() { workerApi->DecreaseInvokeCount(); });
    }
    return CheckConnection(listenWorker, workerApi);
}

Status ObjectClientImpl::MGetH2D(const std::vector<std::string> &objectKeys,
                                 const std::vector<DeviceBlobList> &devBlobList, std::vector<std::string> &failedKeys,
                                 uint64_t timeoutMs)
{
    PerfPoint perfPoint(PerfKey::HETERO_CLIENT_MGET_H2D);
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_HETERO_CLIENT_MGETH2D);
    access.ObjectKeysSummaryRef(objectKeys)
        .DataSizeProvider([&devBlobList] { return CalculateDeviceBlobSize(devBlobList); });

    auto rc = CheckMGetH2DInput(objectKeys, devBlobList);
    if (rc.IsError()) {
        failedKeys.clear();
        access.Result(rc).Record();
        return rc;
    }
    auto cfgRc = UpdateClientRemoteH2DConfig(devBlobList[0].deviceIdx);
    if (cfgRc.IsError()) {
        failedKeys.clear();
        access.Result(cfgRc).Record();
        return cfgRc;
    }
    auto status = MGetH2DImpl(objectKeys, devBlobList, timeoutMs, failedKeys);
    access.Result(status).Record();
    return status;
}

namespace {
std::shared_future<AsyncResult> FastFailAsyncResult(const Status &rc, std::vector<std::string> failedKeys)
{
    std::promise<AsyncResult> promise;
    std::shared_future<AsyncResult> future = promise.get_future().share();
    promise.set_value({ rc, std::move(failedKeys) });
    return future;
}

std::shared_future<AsyncResult> MakeFailedAsyncH2DFuture(ObjectAccessRecorder &access, const Status &rc,
                                                         const std::vector<DeviceBlobList> &devBlobList,
                                                         const std::vector<std::string> &objectKeys,
                                                         std::vector<std::string> failedKeys)
{
    access.ObjectKeysSummaryRef(objectKeys)
        .DataSizeProvider([&devBlobList] { return CalculateDeviceBlobSize(devBlobList); })
        .Result(rc)
        .Record();
    return FastFailAsyncResult(rc, std::move(failedKeys));
}

void GroupH2DObjects(const std::vector<DeviceBlobList> &devBlobList,
                     const std::vector<Buffer *> &existBufferList,
                     std::vector<object_cache::H2DObjectView> &localObjects,
                     std::vector<RemoteH2DGroup> &remoteGroups)
{
    const size_t objectCount = devBlobList.size();
    localObjects.reserve(objectCount);
    std::unordered_map<std::string, size_t> rootInfoToIndexMapping;
    rootInfoToIndexMapping.reserve(objectCount);
    for (size_t i = 0; i < objectCount; i++) {
        auto *buffer = existBufferList[i];
        if (buffer == nullptr) {
            continue;
        }
        object_cache::H2DObjectView view{ &devBlobList[i], buffer, i };
        if (buffer->GetRemoteHostInfo() == nullptr) {
            localObjects.emplace_back(view);
            continue;
        }
        const std::string &rootInternal = buffer->GetRemoteHostInfo()->root_info().internal();
        auto iter = rootInfoToIndexMapping.find(rootInternal);
        if (iter == rootInfoToIndexMapping.end()) {
            iter = rootInfoToIndexMapping.emplace(rootInternal, remoteGroups.size()).first;
            remoteGroups.emplace_back(RemoteH2DGroup{ rootInternal, {} });
            remoteGroups.back().objects.reserve(REMOTE_H2D_GROUP_INITIAL_RESERVE);
        }
        remoteGroups[iter->second].objects.emplace_back(view);
    }
}

Status ValidateDeviceDataCreateBlobs(const std::vector<DeviceBlobList> &devBlobList, int32_t expectedDeviceIdx)
{
    for (size_t i = 0; i < devBlobList.size(); i++) {
        // The device copy manager submits one batch with the first device index; accepting mixed indices here would
        // route part of the batch to the wrong device rather than preserve the old copy-stage failure.
        CHECK_FAIL_RETURN_STATUS(devBlobList[i].deviceIdx == expectedDeviceIdx, K_INVALID,
                                 FormatString("Device index mismatch in batch: expect %d, actual %d, index %zu",
                                              expectedDeviceIdx, devBlobList[i].deviceIdx, i));
        CHECK_FAIL_RETURN_STATUS(
            devBlobList[i].srcOffset >= 0, K_INVALID,
            FormatString("Invalid srcOffset: %d, which must be non-negative.", devBlobList[i].srcOffset));
    }
    return Status::OK();
}

Status ValidateDeviceBlobDeviceIdxBatch(const std::vector<DeviceBlobList> &devBlobList)
{
    CHECK_FAIL_RETURN_STATUS(!devBlobList.empty(), K_INVALID, "The devBlobList is empty");
    const auto expectedDeviceIdx = devBlobList.front().deviceIdx;
    for (size_t i = 1; i < devBlobList.size(); ++i) {
        CHECK_FAIL_RETURN_STATUS(devBlobList[i].deviceIdx == expectedDeviceIdx, K_INVALID,
                                 FormatString("Device index mismatch in batch: expect %d, actual %d, index %zu",
                                              expectedDeviceIdx, devBlobList[i].deviceIdx, i));
    }
    return Status::OK();
}

Status ValidateDeviceDataCreatePayload(const std::vector<DeviceBlobList> &devBlobList,
                                       const std::vector<bool> &exists)
{
    CHECK_FAIL_RETURN_STATUS(devBlobList.size() == exists.size(), K_INVALID,
                             "The size of devBlobList and exists does not match");
    for (size_t i = 0; i < devBlobList.size(); ++i) {
        // Existing objects are filtered before the device copy. Keep accepting their legacy placeholder
        // descriptors (including empty lists, null pointers and zero sizes); only data that will actually be
        // published must describe a valid device memory range.
        if (exists[i]) {
            continue;
        }
        CHECK_FAIL_RETURN_STATUS(!devBlobList[i].blobs.empty(), K_INVALID,
                                 FormatString("DeviceBlobList.blobs cannot be empty, object index: %zu", i));
        for (size_t j = 0; j < devBlobList[i].blobs.size(); ++j) {
            const auto &blob = devBlobList[i].blobs[j];
            CHECK_FAIL_RETURN_STATUS(
                blob.pointer != nullptr, K_INVALID,
                FormatString("Device blob pointer is null, object index: %zu, blob index: %zu", i, j));
            CHECK_FAIL_RETURN_STATUS(
                blob.size > 0, K_INVALID,
                FormatString("Device blob size is zero, object index: %zu, blob index: %zu", i, j));
        }
    }
    return Status::OK();
}

void BuildD2HObjectViews(const std::vector<DeviceBlobList> &devBlobList, const std::vector<bool> &exists,
                         std::vector<std::shared_ptr<Buffer>> &bufferList,
                         std::vector<const DeviceBlobList *> &filteredDeviceBlobRefs,
                         std::vector<D2HObjectView> &d2hObjects)
{
    std::vector<std::shared_ptr<Buffer>> filterBufferList;
    filterBufferList.reserve(bufferList.size());
    filteredDeviceBlobRefs.clear();
    filteredDeviceBlobRefs.reserve(bufferList.size());
    d2hObjects.reserve(bufferList.size());
    for (size_t idx = 0; idx < exists.size(); idx++) {
        if (exists[idx]) {
            continue;
        }
        filterBufferList.emplace_back(std::move(bufferList[idx]));
        // Non-owning ref into devBlobList; the caller-owned list outlives the synchronous compose, copy and publish.
        filteredDeviceBlobRefs.emplace_back(&devBlobList[idx]);
        d2hObjects.emplace_back(D2HObjectView{ filteredDeviceBlobRefs.back(), filterBufferList.back().get(), idx });
    }
    bufferList = std::move(filterBufferList);
}
}  // namespace

std::shared_future<AsyncResult> ObjectClientImpl::AsyncMGetH2D(const std::vector<std::string> &objectKeys,
                                                               const std::vector<DeviceBlobList> &devBlobList,
                                                               uint64_t timeoutMs)
{
    PerfPoint perfPoint(PerfKey::HETERO_CLIENT_ASYNCMGET_H2D);
    auto access = std::make_shared<ObjectAccessRecorder>(
        AccessRecorder::Object(AccessRecorderKey::DS_HETERO_CLIENT_ASYNCMGETH2D));

    auto rc = CheckMGetH2DInput(objectKeys, devBlobList);
    if (rc.IsError()) {
        return MakeFailedAsyncH2DFuture(*access, rc, devBlobList, objectKeys, {});
    }

    auto cfgRc = UpdateClientRemoteH2DConfig(devBlobList[0].deviceIdx);
    if (cfgRc.IsError()) {
        return MakeFailedAsyncH2DFuture(*access, cfgRc, devBlobList, objectKeys, {});
    }

    auto asyncState = std::make_shared<AsyncMGetH2DState>(objectKeys, devBlobList);
    access->ObjectKeysSummaryRef(asyncState->objectKeys)
        .DataSizeProvider([asyncState] { return CalculateDeviceBlobSize(asyncState->devBlobList); });
    std::shared_future<AsyncResult> future = asyncState->promise.get_future().share();

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

    auto copyCompleteTask = [this, traceContext,
        asyncState = std::move(asyncState), access = std::move(access)]() mutable {
        TraceGuard traceGuard = Trace::Instance().SetTraceContext(traceContext);
        auto rc = asyncState->rpcFuture.get();
        if (rc.IsOk()) {
            rc = HostDataCopy2Device(asyncState->devBlobList, asyncState->existBufferList);
        }
        access->Result(rc).Record();
        asyncState->promise.set_value({ rc, asyncState->failedKeys });
    };
    asyncGetCopyPool_->Execute(std::move(copyCompleteTask));
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
    PerfPoint stagePoint(PerfKey::CLIENT_MGET_H2D_GET);
    RETURN_IF_NOT_OK(Get(objectKeys, timeoutMs, bufferList, false, isRH2DSupported));
    stagePoint.RecordAndReset(PerfKey::CLIENT_MGET_H2D_COPY);

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
    // The synchronous caller retains devBlobList until MGetH2D returns, so HostDataCopy2Device may use a
    // read-only view instead of a deep copy.
    auto rc = HostDataCopy2Device(devBlobList, existBufferList);
    stagePoint.Record();
    return rc;
}

Status ObjectClientImpl::PreRegisterDeviceMemory(const std::vector<void *> &data, const std::vector<uint64_t> &dataSize)
{
#ifndef USE_NPU
    (void)data;
    (void)dataSize;
    return Status(K_NOT_SUPPORTED, "RemoteH2D device memory pre-registration is only supported in NPU builds.");
#else
    RETURN_IF_NOT_OK(IsClientReady());
    CHECK_FAIL_RETURN_STATUS(!data.empty(), K_INVALID, "Device memory address list cannot be empty.");
    CHECK_FAIL_RETURN_STATUS(data.size() == dataSize.size(), K_INVALID,
                             FormatString("Device memory address count %zu does not match size count %zu.",
                                          data.size(), dataSize.size()));
    for (size_t i = 0; i < data.size(); ++i) {
        CHECK_FAIL_RETURN_STATUS(data[i] != nullptr, K_INVALID,
                                 FormatString("Device memory address cannot be null, index: %zu.", i));
        CHECK_FAIL_RETURN_STATUS(dataSize[i] > 0, K_INVALID,
                                 FormatString("Device memory size must be greater than 0, index: %zu.", i));
    }

    int32_t deviceId = -1;
    RETURN_IF_NOT_OK(acl::AclDeviceManager::Instance()->GetDeviceIdx(deviceId));
    CHECK_FAIL_RETURN_STATUS(deviceId >= 0, K_INVALID,
                             "Device id is not initialized. Set current device before pre-registering device memory.");
    RETURN_IF_NOT_OK(UpdateClientRemoteH2DConfig(deviceId));
    RETURN_IF_NOT_OK(RemoteH2DManager::Instance().PreRegisterDeviceMemory(data, dataSize));
    {
        std::lock_guard<std::mutex> lock(preRegisteredDeviceMemoryMutex_);
        preRegisteredDeviceMemoryAddrs_.insert(preRegisteredDeviceMemoryAddrs_.end(), data.begin(), data.end());
    }
    return Status::OK();
#endif
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

#ifdef USE_NPU
static Status InitRemoteH2DComm(const std::vector<Buffer *> &existBufferList,
                                std::shared_ptr<RemoteH2DContext> &p2pComm)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(RemoteH2DManager::Instance().SetDeviceIdx(),
                                     "[RH2D][ScatterBatch][Client] SetDeviceIdx failed");
    P2pKind kind = P2P_RECEIVER;
    // Buffers are grouped by data source, so root info should be the same for these objects.
    const auto &rootInfo = existBufferList[0]->GetRemoteHostInfo()->root_info();
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        RemoteH2DManager::Instance().P2PCommInitRootInfo(rootInfo.internal(), rootInfo, kind, p2pComm),
        "[RH2D][ScatterBatch][Client] P2PCommInitRootInfo failed");
    return Status::OK();
}

// The per-entry scatter preparation that previously lived in FillScatterEntry/FillScatterEntries is now
// inlined into ImportSegAndReadHostMemory below as a two-pass flat-storage builder.
static Status ValidateRemoteH2DObjects(const std::vector<H2DObjectView> &objects, size_t &totalDescriptorCount)
{
    totalDescriptorCount = 0;
    for (size_t i = 0; i < objects.size(); i++) {
        const auto &view = objects[i];
        CHECK_FAIL_RETURN_STATUS(view.hostBuffer != nullptr, K_INVALID,
                                 FormatString("RH2D view hostBuffer is null, index=%zu", i));
        CHECK_FAIL_RETURN_STATUS(view.deviceBlobs != nullptr, K_INVALID,
                                 FormatString("RH2D view deviceBlobs is null, index=%zu", i));
        const auto numEl = view.hostBuffer->GetRemoteHostInfo()->data_info().sizes_size();
        const auto blobCount = view.deviceBlobs->blobs.size();
        CHECK_FAIL_RETURN_STATUS(
            static_cast<size_t>(numEl) == blobCount && numEl > 0,
            K_INVALID,
            FormatString("Blobs count mismatch in devBlobList between sender and receiver, sender count is: %ld, "
                         "receiver count is: %ld, mismatch devBlobList index: %zu, mismatch key index: %zu",
                         numEl, blobCount, i, view.requestIndex));
        CHECK_FAIL_RETURN_STATUS(totalDescriptorCount <= SIZE_MAX - static_cast<size_t>(numEl), K_INVALID,
                                 "Total RH2D descriptor count overflows size_t");
        totalDescriptorCount += static_cast<size_t>(numEl);
    }
    return Status::OK();
}

static Status BuildRemoteH2DScatterEntries(const std::vector<H2DObjectView> &objects,
                                           const std::shared_ptr<RemoteH2DContext> &p2pComm,
                                           ScatterBatchStorage &storage)
{
    size_t flatOffset = 0;
    for (size_t i = 0; i < objects.size(); i++) {
        const auto &view = objects[i];
        auto *buffer = view.hostBuffer;
        auto *remoteHostInfo = buffer->GetRemoteHostInfo();
        auto &seg = remoteHostInfo->remote_host_segment();
        auto &hostDataInfo = remoteHostInfo->data_info();
        auto &blobs = view.deviceBlobs->blobs;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            RemoteH2DManager::Instance().ImportHostSegment(p2pComm->remoteEndpoint, seg),
            FormatString("[RH2D][ScatterBatch][Client] ImportHostSegment failed, index=%zu, segLen=%zu, "
                         "dataOffset=%zu",
                         i, seg.seg_len(), seg.seg_data_offset()));
        CHECK_FAIL_RETURN_STATUS(
            seg.seg_data_offset() + hostDataInfo.offset() < seg.seg_len(), K_RUNTIME_ERROR,
            FormatString("The offset overflow, starting point:%zu + blob offset:%zu > segment size:%zu",
                         seg.seg_data_offset(), hostDataInfo.offset(), seg.seg_len()));
        auto &entry = storage.entries[i];
        entry.ddrBuf = reinterpret_cast<void *>(seg.seg_va() + seg.seg_data_offset() + hostDataInfo.offset());
        entry.numEl = hostDataInfo.sizes_size();
        entry.dstBufs = storage.dstBuffers.data() + flatOffset;
        entry.counts = storage.sizes.data() + flatOffset;
        entry.dataType = HCCL_DATA_TYPE_UINT8;
        for (size_t j = 0; j < entry.numEl; j++) {
            auto hostDataSize = hostDataInfo.sizes(j);
            auto deviceDataSize = blobs[j].size;
            CHECK_FAIL_RETURN_STATUS(static_cast<size_t>(hostDataSize) == deviceDataSize, K_RUNTIME_ERROR,
                                     "The data size of device and host is not equal.");
            storage.dstBuffers[flatOffset + j] = blobs[j].pointer;
            storage.sizes[flatOffset + j] = deviceDataSize;
        }
        flatOffset += entry.numEl;
    }
    return Status::OK();
}
#endif  // USE_NPU

static Status ImportSegAndReadHostMemory(const std::vector<H2DObjectView> &objects)
{
    (void)objects;
#ifdef USE_NPU
    // 1. Initialize communicator connection.
    // Note that client uses worker side root info as the key.
    PerfPoint point(PerfKey::CLIENT_IMPORT_SEG_AND_READ);
    std::shared_ptr<RemoteH2DContext> p2pComm;
    // Buffers are grouped by data source, so root info should be the same for these objects.
    std::vector<Buffer *> existBufferList;
    existBufferList.reserve(objects.size());
    for (const auto &view : objects) {
        existBufferList.emplace_back(view.hostBuffer);
    }
    RETURN_IF_NOT_OK(InitRemoteH2DComm(existBufferList, p2pComm));
    size_t totalDescriptorCount = 0;
    RETURN_IF_NOT_OK(ValidateRemoteH2DObjects(objects, totalDescriptorCount));
    ScatterBatchStorage storage;
    storage.entries.resize(objects.size());
    storage.dstBuffers.resize(totalDescriptorCount);
    storage.sizes.resize(totalDescriptorCount);
    RETURN_IF_NOT_OK(BuildRemoteH2DScatterEntries(objects, p2pComm, storage));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        RemoteH2DManager::Instance().ScatterBatch(storage.entries.data(), storage.entries.size(), p2pComm),
        FormatString("[RH2D][ScatterBatch][Client] ScatterBatch failed, entries=%zu", storage.entries.size()));
#endif
    return Status::OK();
}

Status ObjectClientImpl::HostDataCopy2Device(const std::vector<DeviceBlobList> &devBlobList,
                                             std::vector<Buffer *> &existBufferList)
{
    PerfPoint point(PerfKey::CLIENT_H2D_MEMCPY);
    RETURN_IF_NOT_OK(ValidateDeviceBlobDeviceIdxBatch(devBlobList));
    CHECK_FAIL_RETURN_STATUS(devBlobList.size() == existBufferList.size(), K_INVALID,
                             "The size of devBlobList and existBufferList does not match");
    PerfPoint stagePoint(PerfKey::CLIENT_H2D_GROUP_SOURCES);
    if (!IsRemoteH2DEnabled()) {
        // Pure same-node H2D. Build non-owning object views directly from the caller's input without copying
        // DeviceBlobList, then dispatch via the view overload. The synchronous copy finishes before the views expire.
        stagePoint.RecordAndReset(PerfKey::CLIENT_H2D_LOCAL_COPY);
        std::vector<object_cache::H2DObjectView> localObjects;
        localObjects.reserve(devBlobList.size());
        for (size_t i = 0; i < devBlobList.size(); i++) {
            if (existBufferList[i] == nullptr) {
                continue;
            }
            localObjects.emplace_back(object_cache::H2DObjectView{ &devBlobList[i], existBufferList[i], i });
        }
        if (!localObjects.empty()) {
            RETURN_IF_NOT_OK(
                devOcImpl_->MemCopyBetweenDevAndHost(localObjects, workerApi_[LOCAL_WORKER]->enableHugeTlb_));
        }
        stagePoint.RecordAndReset(PerfKey::CLIENT_BATCH_BUFFER_DESTRUCT_GET);
    } else {
        // Group buffers by data source in RH2D scenario with non-owning object views. Views contain only
        // pointers and a requestIndex; no DeviceBlobList or Blob is copied during grouping (design 6.2).
        std::vector<object_cache::H2DObjectView> localObjects;
        std::vector<RemoteH2DGroup> remoteGroups;
        GroupH2DObjects(devBlobList, existBufferList, localObjects, remoteGroups);
        stagePoint.RecordAndReset(PerfKey::CLIENT_H2D_LOCAL_COPY);
        if (!localObjects.empty()) {
            RETURN_IF_NOT_OK(
                devOcImpl_->MemCopyBetweenDevAndHost(localObjects, workerApi_[LOCAL_WORKER]->enableHugeTlb_));
        }
        stagePoint.RecordAndReset(PerfKey::CLIENT_H2D_REMOTE_COPY);
        for (auto &group : remoteGroups) {
            RETURN_IF_NOT_OK(ImportSegAndReadHostMemory(group.objects));
        }
        stagePoint.RecordAndReset(PerfKey::CLIENT_BATCH_BUFFER_DESTRUCT_GET);
    }

    // existBufferList same as bufferList
    existBufferList.clear();
    stagePoint.Record();
    point.Record();
    return Status::OK();
}

Status ObjectClientImpl::DeviceDataCreate(const std::vector<std::string> &objectKeys,
                                          const std::vector<DeviceBlobList> &devBlobList, const SetParam &setParam,
                                          std::vector<std::shared_ptr<Buffer>> &bufferList, std::vector<bool> &exists,
                                          std::vector<const DeviceBlobList *> &filteredDeviceBlobRefs)
{
    PerfPoint point(PerfKey::CLIENT_MULTI_CREATE_OBJECT);
    CHECK_FAIL_RETURN_STATUS(!objectKeys.empty(), K_INVALID, "The keys are empty");
    CHECK_FAIL_RETURN_STATUS(objectKeys.size() == devBlobList.size(), K_INVALID,
                             "The size of objectKeys and devBlobList does not match");
    CHECK_FAIL_RETURN_STATUS(!devBlobList.empty(), K_INVALID, "The devBlobList is empty");

    FullParam param;
    param.writeMode = setParam.writeMode;
    param.cacheType = setParam.cacheType;
    std::vector<size_t> dataSizeList;
    dataSizeList.reserve(objectKeys.size());
    const auto expectedDeviceIdx = devBlobList.front().deviceIdx;
    RETURN_IF_NOT_OK(ValidateDeviceDataCreateBlobs(devBlobList, expectedDeviceIdx));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CheckDeviceValid({ static_cast<uint32_t>(expectedDeviceIdx) }),
                                     "Check device failed.");
    BlobListInfo blobInfo;
    const auto memoryAlignment = workerApi_[LOCAL_WORKER]->GetMemoryAlignment();
    PerfPoint prepareInputPoint(PerfKey::CLIENT_D2H_PREPARE_INPUT);
    RETURN_IF_NOT_OK(PrepareDataSizeList(dataSizeList, devBlobList, blobInfo, memoryAlignment));
    prepareInputPoint.Record();
    VLOG(1) << blobInfo.ToString(true);
    exists.resize(objectKeys.size(), false);
    RETURN_IF_NOT_OK(MultiCreate(objectKeys, dataSizeList, param, false, bufferList, exists));
    RETURN_IF_NOT_OK(ValidateDeviceDataCreatePayload(devBlobList, exists));
    PerfPoint filterPoint(PerfKey::CLIENT_D2H_FILTER_SOURCES);
    std::vector<D2HObjectView> d2hObjects;
    BuildD2HObjectViews(devBlobList, exists, bufferList, filteredDeviceBlobRefs, d2hObjects);
    filterPoint.Record();
    if (bufferList.empty()) {
        return Status::OK();
    }
    point.RecordAndReset(PerfKey::CLIENT_D2H_MEMCPY);
    // Compose from refs and run D2H via the views built during filtering. The Buffer pointees remain stable
    // when filterBufferList is moved into bufferList.
    PerfPoint composePoint(PerfKey::CLIENT_D2H_COMPOSE_PREFIX);
    RETURN_IF_NOT_OK(ComposeBufferDataRefs(filteredDeviceBlobRefs, bufferList, memoryAlignment));
    composePoint.Record();
    RETURN_IF_NOT_OK(devOcImpl_->MemCopyBetweenDevAndHost(d2hObjects, workerApi_[LOCAL_WORKER]->enableHugeTlb_));

    return Status::OK();
}

Status ObjectClientImpl::MSetD2H(const std::vector<std::string> &objectKeys,
                                 const std::vector<DeviceBlobList> &devBlobList, const SetParam &setParam,
                                 std::vector<std::string> *outLocalSetKeys)
{
    PerfPoint perfPoint(PerfKey::HETERO_CLIENT_MSET_D2H);
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_HETERO_CLIENT_MSETD2H);
    access.ObjectKeysSummaryRef(objectKeys)
        .DataSizeProvider([&devBlobList] { return CalculateDeviceBlobSize(devBlobList); });
    if (outLocalSetKeys != nullptr) {
        outLocalSetKeys->clear();
    }
    auto rc = CheckMSetD2HInput(objectKeys, devBlobList, setParam);
    if (rc.IsError()) {
        access.Result(rc).Record();
        return rc;
    }
    // D2H never touches RemoteH2DManager, so this path must not initialize or update RH2D configuration.
    // Device-id consistency is enforced later in DeviceDataCreate: ValidateDeviceDataCreateBlobs checks the
    // whole batch shares one deviceIdx, and CheckDeviceValid verifies that device is available.
    auto status = MSetD2HImpl(objectKeys, devBlobList, setParam, outLocalSetKeys);
    access.Result(status).Record();
    return status;
}

std::shared_future<AsyncResult> ObjectClientImpl::AsyncMSetD2H(const std::vector<std::string> &objectKeys,
                                                               const std::vector<DeviceBlobList> &devBlobList,
                                                               const SetParam &setParam)
{
    PerfPoint perfPoint(PerfKey::HETERO_CLIENT_ASYNCMSET_D2H);
    auto access = std::make_shared<ObjectAccessRecorder>(
        AccessRecorder::Object(AccessRecorderKey::DS_HETERO_CLIENT_ASYNCMSETD2H));
    auto rc = CheckMSetD2HInput(objectKeys, devBlobList, setParam);
    if (rc.IsError()) {
        return MakeFailedAsyncH2DFuture(*access, rc, devBlobList, objectKeys, objectKeys);
    }

    // D2H never touches RemoteH2DManager, so this path must not initialize or update RH2D configuration.
    // Device-id consistency is enforced later in DeviceDataCreate (ValidateDeviceDataCreateBlobs +
    // CheckDeviceValid), matching the synchronous MSetD2H path above.
    auto asyncState = std::make_shared<AsyncMSetD2HState>(objectKeys, devBlobList, setParam);
    access->ObjectKeysSummaryRef(asyncState->objectKeys)
        .DataSizeProvider([asyncState] { return CalculateDeviceBlobSize(asyncState->devBlobList); });

    auto traceContext = Trace::Instance().GetContext();
    return asyncSetRPCPool_->Submit(
        [this, traceContext,
         asyncState = std::move(asyncState), access = std::move(access)]() mutable {
            TraceGuard traceGuard = Trace::Instance().SetTraceContext(traceContext);
            auto rc = MSetD2HImpl(asyncState->objectKeys, asyncState->devBlobList, asyncState->setParam, nullptr);
            access->Result(rc).Record();
            return AsyncResult{ rc, {} };
        });
}

Status ObjectClientImpl::MSetD2HImpl(const std::vector<std::string> &objectKeys,
                                     const std::vector<DeviceBlobList> &devBlobList, const SetParam &setParam,
                                     std::vector<std::string> *outLocalSetKeys)
{
    // Step1: execute Exist check
    std::vector<std::shared_ptr<Buffer>> bufferList;
    std::vector<bool> exists;
    std::vector<const DeviceBlobList *> filteredDeviceBlobRefs;
    RETURN_IF_NOT_OK(DeviceDataCreate(objectKeys, devBlobList, setParam, bufferList, exists, filteredDeviceBlobRefs));

    // If all objects already exist, return success immediately
    if (filteredDeviceBlobRefs.empty()) {
        return Status::OK();
    }
    // Step3: Execute final MultiPublish operation. Serialize blob sizes directly from the filtered
    // device-blob refs; the synchronous request construction does not retain them.
    PerfPoint point(PerfKey::CLIENT_MULTI_PUBLISH_OBJECT);
    return MultiPublish(bufferList, setParam, filteredDeviceBlobRefs, outLocalSetKeys);
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
    std::shared_ptr<client::ListenWorker> listenWorker;
    std::shared_ptr<IClientWorkerApi> workerApi;
    {
        std::lock_guard<std::mutex> lock(switchNodeMutex_);
        listenWorker = id < listenWorker_.size() ? listenWorker_[id] : nullptr;
        workerApi = id < workerApi_.size() ? workerApi_[id] : nullptr;
    }
    return CheckConnection(listenWorker, workerApi);
}

Status ObjectClientImpl::CheckConnection(const std::shared_ptr<client::ListenWorker> &listenWorker,
                                         const std::shared_ptr<IClientWorkerApi> &workerApi)
{
    if (listenWorker == nullptr) {
        return { K_RUNTIME_ERROR,
                 "The current client is abnormal. The listenWorker attribute is empty. Please initialize the client "
                 "again." };
    }
    auto status = listenWorker->CheckWorkerAvailable();
    if (status.IsOk() || enableLocalCache_ || embeddedClientWorkerApi_ != nullptr) {
        return status;
    }
    if (workerApi == nullptr) {
        return status;
    }
    if (ProbeTcpPort(workerApi->hostPort_, BOUND_WORKER_PROBE_TIMEOUT_MS) != TcpPortProbeResult::PEER_DEAD) {
        return status;
    }
    return { K_RPC_PEER_DEAD,
             FormatString("Client connection to bound worker %s is broken.", workerApi->hostPort_.ToString()) };
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
    ApiDeadlineGuard deadlineGuard(requestTimeoutMs_);
    CHECK_FAIL_RETURN_STATUS(!objectKey.empty(), K_INVALID, "The objectKey is empty");
    RETURN_IF_NOT_OK(CheckValidObjectKey(objectKey));
    CHECK_FAIL_RETURN_STATUS(dataSize > 0, K_INVALID, "The dataSize value should be bigger than zero.");
    auto config = GetClientLatencyTraceConfig();
    const bool traceEnabled = ShouldCollectLatencyTrace(config);
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_CREATE_START);
    }
    PerfPoint createPoint(PerfKey::CLIENT_CREATE_OBJECT);
    VLOG(1) << "Begin to create object, object_key: " << objectKey;
    buffer.reset();  // Decrease should precede increase to avoid worker lost (ref cnt will be clear) and then restart.
    std::shared_ptr<Buffer> newBuffer;
    if (!enableLocalCache_) {
        RETURN_IF_NOT_OK(CreateRoutedBuffer(objectKey, dataSize, param, newBuffer));
    } else {
        std::shared_ptr<IClientWorkerApi> workerApi;
        std::unique_ptr<Raii> raii;
        RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
        // Route cross-host bound workers through the transport layer (UB/TCP) instead of the SHM
        // bound-worker path
        if (transportLayer_ != nullptr && !transportLayer_->IsSameHostWorker(workerApi->hostPort_)) {
            RETURN_IF_NOT_OK(CreateRoutedBuffer(objectKey, dataSize, param, newBuffer));
        } else {
            RETURN_IF_NOT_OK(
                boundMode_->CreateShmBuffer(objectKey, dataSize, param, workerApi, config, traceEnabled, newBuffer));
        }
    }
    buffer = std::move(newBuffer);
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_CREATE_END);
    }
    EmitClientLatencySummary(LatencyTickKey::CLIENT_CREATE_START, LatencyTickKey::CLIENT_CREATE_END);
    createPoint.Record();
    VLOG(1) << "Finished creating object, object_key: " << objectKey;
    return Status::OK();
}

Status ObjectClientImpl::CreateRoutedBuffer(const std::string &objectKey, uint64_t dataSize,
                                            const FullParam &param, std::shared_ptr<Buffer> &buffer)
{
    RETURN_RUNTIME_ERROR_IF_NULL(transportLayer_);
    SetRouteContext routeContext;
    RETURN_IF_NOT_OK(SelectSetRoute(objectKey, {}, routeContext));
    const auto requestContext = BuildTransportRequestContext(routeContext);
    client::TransportCreateParam createParam;
    createParam.requestContext = requestContext;
    createParam.cacheType = param.cacheType;
    createParam.consistencyType = param.consistencyType;
    createParam.writeMode = param.writeMode;
    createParam.subTimeoutMs = requestTimeoutMs_;
    std::shared_ptr<ObjectBuffer> objBuf;
    RETURN_IF_NOT_OK(transportLayer_->Create(routeContext.worker, objectKey, dataSize, createParam, objBuf));
    // Bridge: transfer the routed ObjectBufferInfo (populated by ShmTransporter::Create with
    // workerAddr/shmId/pointer/mmapEntry/sessionLockId/receiveBufferOwner) to a legacy Buffer.
    auto bufferInfo = ObjectBufferInternal::ExtractInfo(objBuf);
    bufferInfo->isRoutedWrite = true;  // marks a routed write buffer (not a Get'd read-only buffer)
    auto rc = Buffer::CreateBuffer(bufferInfo, shared_from_this(), buffer);
    if (rc.IsError() && bufferInfo->receiveBufferOwner != nullptr) {
        // Buffer init failed (rare); no Buffer will release the worker allocation, so retire it here.
        bufferInfo->receiveBufferOwner->Release();
    }
    return rc;
}

Status ObjectClientImpl::MultiCreateRouted(const std::vector<std::string> &objectKeyList,
                                           const std::vector<uint64_t> &dataSizeList, const FullParam &param,
                                           std::vector<std::shared_ptr<Buffer>> &bufferList,
                                           std::vector<bool> &exists)
{
    RETURN_RUNTIME_ERROR_IF_NULL(transportLayer_);
    const auto sz = objectKeyList.size();
    bufferList.assign(sz, nullptr);
    // Routed MCreate does not pre-check existence; existence is enforced at MSet/Publish time
    // (consistent with the one-step routed MSet, which checks existence at transportLayer_->MSet).
    exists.assign(sz, false);
    auto routing = std::atomic_load(&routing_);
    RETURN_RUNTIME_ERROR_IF_NULL(routing);
    std::unordered_map<HostPort, std::vector<std::string>> groupedKeys;
    RETURN_IF_NOT_OK(routing->SelectWorkers(objectKeyList, dataPlacementPolicy_, groupedKeys));
    // Map each key back to its original position so results land in the caller's order.
    std::unordered_map<std::string, size_t> keyIndex;
    keyIndex.reserve(sz);
    for (size_t i = 0; i < sz; ++i) {
        keyIndex.emplace(objectKeyList[i], i);
    }
    CHECK_FAIL_RETURN_STATUS(keyIndex.size() == sz, K_INVALID,
                             "MultiCreate routed path does not support duplicate keys");
    // Retire worker allocations behind already-created routed Buffers when a later group fails.
    // IReceiveBufferOwner::Release is idempotent, so the Buffer destructor's later Release is a no-op.
    auto releaseAllocated = [&bufferList]() {
        for (auto &b : bufferList) {
            if (b != nullptr && b->bufferInfo_ != nullptr && b->bufferInfo_->receiveBufferOwner != nullptr) {
                b->bufferInfo_->receiveBufferOwner->Release();
            }
        }
    };
    for (auto &entry : groupedKeys) {
        std::vector<uint64_t> sizes;
        sizes.reserve(entry.second.size());
        for (const auto &key : entry.second) {
            sizes.emplace_back(dataSizeList[keyIndex[key]]);  // key from SelectWorkers(input) is in keyIndex
        }
        auto rc = ProcessRoutedMCreateGroup(entry.first, entry.second, sizes, param, keyIndex, bufferList);
        if (rc.IsError()) {
            releaseAllocated();
            bufferList.clear();
            return rc;
        }
    }
    return Status::OK();
}

Status ObjectClientImpl::ProcessRoutedMCreateGroup(const HostPort &worker, const std::vector<std::string> &keys,
                                                   const std::vector<uint64_t> &sizes, const FullParam &param,
                                                   const std::unordered_map<std::string, size_t> &keyIndex,
                                                   std::vector<std::shared_ptr<Buffer>> &bufferList)
{
    SetRouteContext routeContext;
    RETURN_IF_NOT_OK(BuildSetRouteContext(worker, routeContext));
    const auto requestContext = BuildTransportRequestContext(routeContext);
    client::TransportCreateParam createParam;
    createParam.requestContext = requestContext;
    createParam.cacheType = param.cacheType;
    createParam.consistencyType = param.consistencyType;
    createParam.writeMode = param.writeMode;
    createParam.subTimeoutMs = requestTimeoutMs_;
    std::vector<std::shared_ptr<ObjectBuffer>> objBufs;
    RETURN_IF_NOT_OK(transportLayer_->MCreate(worker, keys, sizes, createParam, objBufs));
    for (auto &objBuf : objBufs) {
        // Bridge: transfer the routed ObjectBufferInfo to a legacy Buffer at its original index.
        auto bufferInfo = ObjectBufferInternal::ExtractInfo(objBuf);
        bufferInfo->isRoutedWrite = true;  // marks a routed write buffer (not a Get'd read-only buffer)
        bufferInfo->ttlSecond = param.ttlSecond;          // carry Create-time ttl/existence to Publish
        bufferInfo->existence = static_cast<int>(param.existence);
        auto it = keyIndex.find(bufferInfo->objectKey);
        if (it == keyIndex.end()) {
            if (bufferInfo->receiveBufferOwner != nullptr) {
                bufferInfo->receiveBufferOwner->Release();
            }
            return Status(K_RUNTIME_ERROR, "Routed MCreate returned an unknown object key");
        }
        auto rc = Buffer::CreateBuffer(bufferInfo, shared_from_this(), bufferList[it->second]);
        if (rc.IsError()) {
            // Buffer init failed (rare); no Buffer will own it, so retire the worker allocation here.
            if (bufferInfo->receiveBufferOwner != nullptr) {
                bufferInfo->receiveBufferOwner->Release();
            }
            return rc;
        }
    }
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
        boundMode_->ConstructMultiCreateParam(objectKeyList, dataSizeList, bufferList, multiCreateParamList,
                                           dataSizeSum));
    point.Record();
    if (!enableLocalCache_) {
        // Route each buffer to its hash-ring-selected worker via the transport layer.
        return MultiCreateRouted(objectKeyList, dataSizeList, param, bufferList, exists);
    }
    // If failed with create, need to rollback.
    auto version = 0u;
    // This variable is the output from MultiCreate, indicates whether shared memory was actually used
    auto useShmTransfer = false;
    // Pre-condition check for whether we should attempt shared memory or UB
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    if (IsUrmaEnabled() && !workerApi->IsShmEnable()) {
        RETURN_RUNTIME_ERROR_IF_NULL(transportLayer_);
        RETURN_IF_NOT_OK(transportLayer_->CheckLocalUbSenderAdmission());
    }
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
                auto bufferInfo = boundMode_->MakeObjectBufferInfo(objectKeyList[i], nullptr, 0, 0, param, false, 0);
                std::shared_ptr<Buffer> placeholder;
                RETURN_IF_NOT_OK(Buffer::CreateBuffer(bufferInfo, shared_from_this(), placeholder));
                bufferList[i] = std::move(placeholder);
                continue;
            }
            auto &objectKey = objectKeyList[i];
            auto dataSize = dataSizeList[i];
            auto version = 0u;
            std::shared_ptr<Buffer> newBuffer;
            auto bufferInfo = boundMode_->MakeObjectBufferInfo(objectKey, nullptr, dataSize, 0, param, false, version);
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
    RETURN_IF_NOT_OK(
        boundMode_->MutiCreateParallel(skipCheckExistence, param, version, exists, multiCreateParamList, bufferList));
    isInactive = true;
    return Status::OK();
}

Status ObjectClientImpl::UpdateToken(SensitiveValue &token)
{
    SensitiveValue tokenCopy(token);
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    RETURN_IF_NOT_OK(workerApi->UpdateToken(token));
    std::atomic_store(&transportToken_, std::make_shared<const SensitiveValue>(std::move(tokenCopy)));
    return Status::OK();
}

Status ObjectClientImpl::UpdateAkSk(const std::string &accessKey, SensitiveValue &secretKey)
{
    if (!enableLocalCache_) {
        return Status(K_NOT_SUPPORTED, "UpdateAkSk is not supported when local cache is disabled");
    }
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    SensitiveValue transportSecretKey(secretKey);
    RETURN_IF_NOT_OK(workerApi->UpdateAkSk(accessKey, secretKey));
    RETURN_RUNTIME_ERROR_IF_NULL(transportSignature_);
    return transportSignature_->SetClientAkSk(accessKey, std::move(transportSecretKey));
}

Status ObjectClientImpl::UpdateConfig(const std::string &configJson)
{
    {
        std::lock_guard<std::mutex> lock(g_kvClientConfigMutex);
        if (g_hasKvClientProcessConfig) {
            auto it = g_kvClientProcessConfig.find("monitor_config_file");
            if (it != g_kvClientProcessConfig.end() && !it->second.empty()) {
                const std::string reason =
                    "UpdateConfig: MonitorConfigPath must be empty when using UpdateConfig API";
                OperationLogger::Instance().LogConfigApiFailed("UpdateConfig", reason);
                return Status(StatusCode::K_INVALID, reason);
            }
        }
    }
    DynamicConfigUpdater updater(FlagsMonitor::GetInstance()->GetDynamicFlagConfig());
    return updater.ApplyJson(configJson, "UpdateConfig");
}

Status ObjectClientImpl::PublishRoutedBuffer(const std::shared_ptr<ObjectBufferInfo> &bufferInfo,
                                             const std::unordered_set<std::string> &nestedObjectKeys, bool isSeal)
{
    RETURN_RUNTIME_ERROR_IF_NULL(transportLayer_);
    if (bufferInfo->routedWriteSourceDraining) {
        auto rc = ReplayRoutedBuffer(bufferInfo, nestedObjectKeys, isSeal);
        if (rc.IsOk()) {
            bufferInfo->isSeal = isSeal;
        }
        return rc;
    }
    // The worker was pinned on the buffer at Create time; rebuild the route context from it only to
    // carry the client identity (clientId/token/tenantId) into the request context. TransportLayer::Set
    // reads the target worker from the buffer's own workerAddr.
    SetRouteContext routeContext;
    RETURN_IF_NOT_OK(BuildSetRouteContext(bufferInfo->workerAddr, routeContext));
    const auto requestContext = BuildTransportRequestContext(routeContext);
    std::shared_ptr<ObjectBuffer> objBuf;
    RETURN_IF_NOT_OK(ObjectBufferInternal::Create(bufferInfo, objBuf));
    // The legacy Buffer owns the payload; keep the transient ObjectBuffer from freeing (and nulling)
    // the shared pointer at end of scope. No-op for SHM buffers.
    ObjectBufferInternal::DisownLocalMemory(*objBuf);
    client::TransportSetParam setParam;
    setParam.requestContext = requestContext;
    setParam.nestedKeys = nestedObjectKeys;
    setParam.ttlSecond = bufferInfo->ttlSecond;
    setParam.existence = static_cast<ExistenceOpt>(bufferInfo->existence);
    setParam.isSeal = isSeal;
    setParam.subTimeoutMs = requestTimeoutMs_;
    auto setRc = transportLayer_->Set(*objBuf, setParam);
    if (setRc.GetCode() == K_SCALE_DOWN) {
        setRc = ReplayRoutedBuffer(bufferInfo, nestedObjectKeys, isSeal);
    }
    if (setRc.IsOk()) {
        bufferInfo->isSeal = isSeal;  // mark sealed only after a successful Set (avoid stuck-sealed on retry)
    }
    return setRc;
}

Status ObjectClientImpl::ReplayRoutedBuffer(const std::shared_ptr<ObjectBufferInfo> &bufferInfo,
                                            const std::unordered_set<std::string> &nestedObjectKeys, bool isSeal)
{
    bufferInfo->routedWriteSourceDraining = true;
    CHECK_FAIL_RETURN_STATUS(bufferInfo->pointer != nullptr, K_RUNTIME_ERROR,
                             "Routed buffer has no payload for ScaleIn replay");
    FullParam param;
    param.cacheType = bufferInfo->objectMode.GetCacheType();
    param.consistencyType = bufferInfo->objectMode.GetConsistencyType();
    param.writeMode = bufferInfo->objectMode.GetWriteMode();
    param.ttlSecond = bufferInfo->ttlSecond;
    param.existence = static_cast<ExistenceOpt>(bufferInfo->existence);
    const auto *data = bufferInfo->pointer + bufferInfo->metadataSize;
    std::vector<HostPort> excludedWorkers{ bufferInfo->workerAddr };
    return ExecuteSetFlow(bufferInfo->objectKey, data, bufferInfo->dataSize, param, nestedObjectKeys,
                          bufferInfo->ttlSecond, bufferInfo->existence, requestTimeoutMs_, isSeal,
                          std::move(excludedWorkers));
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

Status ObjectClientImpl::SelectSetRoute(const std::string &objectKey,
                                        const std::vector<HostPort> &excludedWorkers,
                                        SetRouteContext &routeContext)
{
    const auto effectiveExclusions = MergeWriteTargetExclusions(excludedWorkers);
    SetRouteContext selected;
    if (enableLocalCache_) {
        RETURN_IF_NOT_OK(GetAvailableWorkerApi(selected.clientApi, selected.invokeGuard));
        selected.worker = selected.clientApi->hostPort_;
        const bool excludedForThisRequest =
            std::find(excludedWorkers.begin(), excludedWorkers.end(), selected.worker) != excludedWorkers.end();
        if (!excludedForThisRequest) {
            const bool ubWriteTargetQuarantined =
                std::find(effectiveExclusions.begin(), effectiveExclusions.end(), selected.worker)
                != effectiveExclusions.end();
            if (!ubWriteTargetQuarantined || selected.clientApi->IsShmEnable()) {
                selected.directWorkerApi = selected.clientApi;
                routeContext = std::move(selected);
                return Status::OK();
            }
        }
        selected.invokeGuard.reset();
        selected.clientApi.reset();
    }
    auto routing = std::atomic_load(&routing_);
    RETURN_RUNTIME_ERROR_IF_NULL(routing);
    HostPort worker;
    RETURN_IF_NOT_OK(routing->SelectWorker(objectKey, dataPlacementPolicy_, worker, effectiveExclusions));
    return BuildSetRouteContext(worker, routeContext);
}

std::vector<HostPort> ObjectClientImpl::MergeWriteTargetExclusions(
    const std::vector<HostPort> &excludedWorkers) const
{
    std::vector<HostPort> result = excludedWorkers;
    if (ubHealthFilter_ == nullptr) {
        return result;
    }
    for (const auto &worker : ubHealthFilter_->GetUnavailableWriteTargets()) {
        if (std::find(result.begin(), result.end(), worker) == result.end()) {
            result.emplace_back(worker);
            LOG(WARNING) << "[CLIENT_UB_WRITE_TARGET_EXCLUDED] Quarantined UB write target excluded from this "
                            "Set/MSet routing, worker="
                         << worker.ToString();
        }
    }
    return result;
}

Status ObjectClientImpl::BuildSetRouteContext(const HostPort &worker, SetRouteContext &routeContext)
{
    SetRouteContext selected;
    selected.worker = worker;
    std::shared_ptr<client::ListenWorker> listenWorker;
    {
        std::lock_guard<std::mutex> lock(switchNodeMutex_);
        const auto node = currentNode_.load();
        CHECK_FAIL_RETURN_STATUS(node < workerApi_.size() && workerApi_[node] != nullptr, K_NOT_READY,
                                 "No client identity is available for routed Set");
        listenWorker = node < listenWorker_.size() ? listenWorker_[node] : nullptr;
        selected.clientApi = workerApi_[node];
        selected.clientApi->IncreaseInvokeCount();
        selected.invokeGuard =
            std::make_unique<Raii>([api = selected.clientApi]() { api->DecreaseInvokeCount(); });
    }
    if (enableLocalCache_) {
        RETURN_IF_NOT_OK(CheckConnection(listenWorker, selected.clientApi));
    }
    if (selected.worker == selected.clientApi->hostPort_) {
        selected.directWorkerApi = selected.clientApi;
    }
    routeContext = std::move(selected);
    return Status::OK();
}

client::TransportRequestContext ObjectClientImpl::BuildTransportRequestContext(
    const SetRouteContext &routeContext) const
{
    client::TransportRequestContext context;
    context.clientId = routeContext.clientApi->clientId_;
    const auto token = std::atomic_load(&transportToken_);
    if (token != nullptr && !token->Empty()) {
        context.token.assign(token->GetData(), token->GetSize());
    }
    const auto &requestTenantId = GetRequestContext()->tenantId;
    context.tenantId = requestTenantId.empty() ? tenantId_ : requestTenantId;
    return context;
}

Status ObjectClientImpl::ProcessTransportPut(
    const std::string &objectKey, const uint8_t *data, uint64_t size, const FullParam &param,
    const std::unordered_set<std::string> &nestedObjectKeys, uint32_t ttlSecond, int existence,
    const SetRouteContext &routeContext, SetFailureStage &failureStage,
    client::TransportSetResult &transportResult, int32_t requestTimeoutMs, bool isSeal)
{
    RETURN_RUNTIME_ERROR_IF_NULL(transportLayer_);
    const int32_t subTimeoutMs = requestTimeoutMs > 0 ? requestTimeoutMs : requestTimeoutMs_;
    const auto requestContext = BuildTransportRequestContext(routeContext);
    client::TransportCreateParam createParam;
    createParam.requestContext = requestContext;
    createParam.cacheType = param.cacheType;
    createParam.consistencyType = param.consistencyType;
    createParam.writeMode = param.writeMode;
    createParam.subTimeoutMs = subTimeoutMs;
    failureStage = SetFailureStage::CREATE;
    std::shared_ptr<ObjectBuffer> buffer;
    RETURN_IF_NOT_OK(transportLayer_->Create(routeContext.worker, objectKey, size, createParam, buffer));

    failureStage = SetFailureStage::TRANSFER;
    const bool traceEnabled = IsClientLatencyTraceActive();
    AddLatencyTickIfEnabled(traceEnabled, LatencyTickKey::CLIENT_MEMORY_COPY_START);
    auto *dst = static_cast<uint8_t *>(buffer->MutableData());
    Status copyRc = size == 0 || setMemoryCopyThreadPool_ == nullptr || size <= setMemcpyParallelThreshold_
                        ? HugeMemoryCopy(dst, buffer->GetSize(), data, size)
                        : ::datasystem::MemoryCopy(dst, buffer->GetSize(), data, size, setMemoryCopyThreadPool_,
                                                   setMemcpyParallelThreshold_);
    AddLatencyTickIfEnabled(traceEnabled, LatencyTickKey::CLIENT_MEMORY_COPY_END);
    if (copyRc.IsError()) {
        LOG_IF_ERROR(transportLayer_->Release(*buffer, requestContext),
                     "Release routed Set allocation after MemoryCopy failure failed");
        return copyRc;
    }
    client::TransportSetParam setParam;
    setParam.requestContext = requestContext;
    setParam.nestedKeys = nestedObjectKeys;
    setParam.ttlSecond = ttlSecond;
    setParam.existence = static_cast<ExistenceOpt>(existence);
    setParam.isSeal = isSeal;
    setParam.subTimeoutMs = subTimeoutMs;
    failureStage = SetFailureStage::PUBLISH;
    Status setRc = transportLayer_->Set(*buffer, setParam, transportResult);
    if (setRc.GetCode() == K_URMA_NEED_CONNECT) {
        // TransportLayer returns this only after same-worker UB reconnect failed, before Publish was sent.
        failureStage = SetFailureStage::TRANSFER;
    }
    return setRc;
}

bool ObjectClientImpl::HandleSetRouteFailure(const Status &status, SetFailureStage failureStage,
                                             const HostPort &worker, std::vector<HostPort> &excludedWorkers,
                                             bool safeWriteTargetReplay)
{
    auto routing = std::atomic_load(&routing_);
    if (routing == nullptr) {
        return false;
    }
    auto excludeWorker = [&excludedWorkers, &worker]() {
        if (std::find(excludedWorkers.begin(), excludedWorkers.end(), worker) == excludedWorkers.end()) {
            excludedWorkers.emplace_back(worker);
        }
    };
    if (status.GetCode() == K_METADATA_OWNER_UNAVAILABLE) {
        if (routing->ForceRefresh()) {
            LOG(INFO) << "[Routing] Force hash ring refresh after metadata owner RPC failure, ingress worker: "
                      << worker.ToString();
        }
        return false;
    }
    if (status.GetCode() == K_SCALE_DOWN) {
        excludeWorker();
        return true;
    }
    // CANCELLED (brpc-internal cancellation) is excluded: it is not a transport
    // connection failure, so it must not evict the worker from the routing table.
    // DEADLINE_EXCEEDED is also excluded: it means the request budget is exhausted, not that
    // the peer is unreachable. Retrying on another worker would only burn more time past the
    // original deadline (root cause of SDK Set max 436ms: multiple retry rounds accumulating).
    const bool connectionFailure = status.GetCode() == K_CLIENT_WORKER_DISCONNECT
                                   || (IsRetryableRpcError(status)
                                       && status.GetCode() != K_RPC_CANCELLED
                                       && status.GetCode() != K_RPC_DEADLINE_EXCEEDED)
                                   || IsNonRetryableRpcError(status);
    const bool transferFailure = status.GetCode() == K_URMA_NEED_CONNECT;
    const bool workerNotReady = status.GetCode() == K_NOT_READY
                                && (failureStage == SetFailureStage::CREATE
                                    || failureStage == SetFailureStage::PUBLISH);
    const bool publishNotSent = failureStage == SetFailureStage::PUBLISH
                                && IsBrpcRequestDefinitelyNotSent(status);
    // Global eviction (BrokenFilter, 3s TTL): only genuine peer failures (IsRoutingEvictionFailure)
    // or K_NOT_READY. Transient errors still steer THIS request via retry/excludeWorker but must not
    // evict globally (code=37, 083cc75bd4 regression).
    if (IsRoutingEvictionFailure(status) || workerNotReady) {
        routing->UpdateState(worker, K_CLIENT_WORKER_DISCONNECT);
    }
    const bool retry = safeWriteTargetReplay
                       || (failureStage == SetFailureStage::CREATE && (connectionFailure || workerNotReady))
                       || (failureStage == SetFailureStage::TRANSFER && transferFailure)
                       || (failureStage == SetFailureStage::PUBLISH && (workerNotReady || publishNotSent));
    if (retry) {
        excludeWorker();
    }
    // An unmarked Publish connection error is ambiguous and must not be replayed on another worker.
    return retry;
}

Status ObjectClientImpl::ExecuteSetFlow(
    const std::string &objectKey, const uint8_t *data, uint64_t size, const FullParam &param,
    const std::unordered_set<std::string> &nestedObjectKeys, uint32_t ttlSecond, int existence,
    int32_t requestTimeoutMs, bool isSeal, std::vector<HostPort> excludedWorkers)
{
    Status rc(K_RUNTIME_ERROR, "Set route attempts exhausted");
    for (size_t attempt = 0; attempt < SET_ROUTE_MAX_ATTEMPTS; ++attempt) {
        RETURN_IF_NOT_OK(ApiDeadline::Instance().CheckApiDeadline());
        SetRouteContext routeContext;
        RETURN_IF_NOT_OK(SelectSetRoute(objectKey, excludedWorkers, routeContext));
        VLOG(1) << FormatString("[Set] attempt: %zu, objectKey: %s, clientId: %s, worker: %s", attempt + 1,
                                objectKey, routeContext.clientApi->clientId_, routeContext.worker.ToString());
        SetFailureStage failureStage = SetFailureStage::CREATE;
        client::TransportSetResult transportResult;
        // Branch 1 is a local-cache shortcut that writes via the bound worker's SHM. In routed mode
        // (enableLocalCache_ == false) every write must go through the transport layer (Branch 3) for
        // uniform placement and shm lifecycle, so gate this shortcut on enableLocalCache_. Without the
        // gate, a key whose route lands on the bound worker over a SHM-capable connection with
        // size >= threshold would wrongly take ProcessShmPut.
        if (!isSeal && enableLocalCache_ && routeContext.directWorkerApi != nullptr
            && routeContext.directWorkerApi->ShmCreateable(size)) {
            rc = boundMode_->ProcessShmPut(objectKey, data, size, param, nestedObjectKeys, ttlSecond,
                                           routeContext.directWorkerApi, existence, failureStage, requestTimeoutMs);
            if (rc.IsOk() || !HandleSetRouteFailure(rc, failureStage, routeContext.worker, excludedWorkers)) {
                return rc;
            }
            continue;
        }
        if (!isSeal && routeContext.directWorkerApi != nullptr && transportLayer_ == nullptr) {
            return boundMode_->ProcessDirectSetWithoutTransport(objectKey, data, size, param, nestedObjectKeys,
                                                                ttlSecond, existence, routeContext, failureStage,
                                                                excludedWorkers, requestTimeoutMs);
        }
        rc = ProcessTransportPut(objectKey, data, size, param, nestedObjectKeys, ttlSecond, existence,
                                 routeContext, failureStage, transportResult, requestTimeoutMs, isSeal);
        if (rc.IsOk()
            || !HandleSetRouteFailure(rc, failureStage, routeContext.worker, excludedWorkers,
                                      transportResult.writeTargetQuarantined
                                          && (!transportResult.publishAttempted
                                              || transportResult.publishDefinitelyNotSent))) {
            return rc;
        }
    }
    return rc;
}

Status ObjectClientImpl::Put(const std::string &objectKey, const uint8_t *data, uint64_t size, const FullParam &param,
                             const std::unordered_set<std::string> &nestedObjectKeys, uint32_t ttlSecond, int existence,
                             int32_t requestTimeoutMs)
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
    auto config = GetClientLatencyTraceConfig();
    const bool traceEnabled = ShouldCollectLatencyTrace(config);
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_SET_START);
    }
    const int32_t effectiveTimeoutMs = requestTimeoutMs > 0 ? requestTimeoutMs : requestTimeoutMs_;
    ApiDeadlineGuard deadlineGuard(effectiveTimeoutMs);
    GetRequestContext()->reqTimeoutDuration.InitUs(ApiDeadline::Instance().ApiRemainingUs());
    Timer setTimer;
    Status rc = ExecuteSetFlow(objectKey, data, size, param, nestedObjectKeys, ttlSecond, existence,
                               requestTimeoutMs);
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_SET_END);
    }
    EmitClientLatencySummary(LatencyTickKey::CLIENT_SET_START, LatencyTickKey::CLIENT_SET_END);
    const auto totalUs = static_cast<uint64_t>(setTimer.ElapsedMicroSecond());
    SLOW_LOG_IF_OR_VLOG(INFO, config.processSlowerThanUs > 0 && totalUs >= config.processSlowerThanUs, 1,
                        FormatString("[Set] Done, objectKey: %s, totalCost: %.3fms, status: %s", objectKey,
                                     static_cast<double>(totalUs) / US_PER_MS, rc.ToString()));
    return rc;
}

#ifdef BUILD_PIPLN_H2D

static inline void RecordFailedPipelineKey(const std::string &key, std::shared_ptr<H2DChunkManager> chunkManager,
                                           std::vector<std::string> &failedKeys, const std::string &msg)
{
    LOG(ERROR) << key << " failed:" << msg;
    chunkManager->MarkCancelOrDone(key, false /* isDone */);
    failedKeys.emplace_back(key);
}

bool ObjectClientImpl::PostProcessShmPipelineKey(const std::string &objectKey, const GetRspPb::ObjectInfoPb &info,
                                                 const std::shared_ptr<H2DChunkManager> &chunkManager, uint32_t version,
                                                 std::shared_ptr<Buffer> &buffer, std::vector<std::string> &failedKeys)
{
    if (info.store_fd() == -1) {
        RecordFailedPipelineKey(objectKey, chunkManager, failedKeys, "shmem fd is -1 in in pipeline rh2d response");
        return false;
    }
    if (info.has_host_info()) {
        // Special case for Remote H2D scenario.
        RecordFailedPipelineKey(objectKey, chunkManager, failedKeys,
                                "server tell host_info in pipeline rh2d response, which should be a bug");
        return false;
    }
    Status status = boundMode_->SetShmObjectBuffer(objectKey, info, version, buffer);
    if (status.IsError()) {
        RecordFailedPipelineKey(objectKey, chunkManager, failedKeys, "SetShmObjectBuffer failed");
        return false;
    }
    if (info.pipeline_done_step() != PIPLN_DONE_TWO_STEP) {
        RecordFailedPipelineKey(objectKey, chunkManager, failedKeys,
                                std::string("pipeline step at ") + std::to_string(info.pipeline_done_step()));
        return false;
    }
    return true;
}

bool ObjectClientImpl::PostProcessNonShmPipelineKey(const std::string &objectKey,
                                                    const GetRspPb::PayloadInfoPb &payloadInfo,
                                                    const std::shared_ptr<H2DChunkManager> &chunkManager,
                                                    uint64_t reqId, uint32_t version, std::vector<RpcMessage> &payloads,
                                                    std::shared_ptr<Buffer> &buffer,
                                                    std::vector<std::string> &failedKeys)
{
    METRIC_ADD(metrics::KvMetricId::CLIENT_GET_TCP_READ_TOTAL_BYTES, static_cast<uint64_t>(payloadInfo.data_size()));
    Status status = boundMode_->SetNonShmObjectBuffer(objectKey, payloadInfo, version, payloads, buffer);
    if (status.IsError()) {
        RecordFailedPipelineKey(objectKey, chunkManager, failedKeys, "SetShmObjectBuffer failed");
        return false;
    }
    OsXprtPipln::ChunkTag tag{ .reqId = reqId, .chunkType = OsXprtPipln::ChunkTag::lastChunkTag, .chunkId = 0 };
    OsXprtPipln::ChunkTag::SetObjectSize(tag, buffer->GetSize());
    chunkManager->DoPiplnStep2_ChunkConsume(reqId, reinterpret_cast<uint64_t>(buffer->ImmutableData()), tag,
                                            buffer->GetSize());
    chunkManager->MarkCancelOrDone(reqId, false /* isDone */);
    return true;
}

std::vector<std::pair<std::string *, uint64_t>> ObjectClientImpl::PostProcessPipelineKeys(
    std::vector<std::string> &objectKeys, GetRspPb &rsp, PiplnRh2dParam &piplnRh2dParam, uint32_t version,
    std::vector<std::string> &failedKeys)
{
    std::vector<std::pair<std::string *, uint64_t>> needWaitKeysIds;
    std::shared_ptr<H2DChunkManager> chunkManager = piplnRh2dParam.chunkManager;
    auto &buffers = piplnRh2dParam.buffers;
    buffers.resize(objectKeys.size(), { nullptr });

    size_t i = 0;
    size_t j = 0;
    size_t shmCount = static_cast<size_t>(rsp.objects().size());
    size_t noShmCount = static_cast<size_t>(rsp.payload_info().size());
    for (size_t index = 0; index < (size_t)rsp.objects_size(); index++) {
        std::string &objectKey = objectKeys[index];
        uint64_t reqId;
        chunkManager->GetReqId(objectKey, reqId);

        std::shared_ptr<Buffer> &buffer = buffers[index];
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
            if (PostProcessShmPipelineKey(objectKey, info, chunkManager, version, buffer, failedKeys)) {
                needWaitKeysIds.emplace_back(std::make_pair(&objectKey, reqId));
            }
        } else if (isNoShm) {
            const GetRspPb::PayloadInfoPb &payloadInfo = rsp.payload_info(j);
            j++;
            if (PostProcessNonShmPipelineKey(objectKey, payloadInfo, chunkManager, reqId, version,
                                             piplnRh2dParam.payloads, buffer, failedKeys)) {
                needWaitKeysIds.emplace_back(&objectKey, reqId);
            }
        } else {
            RecordFailedPipelineKey(objectKey, chunkManager, failedKeys, "Object key does not match with GetRspPb");
        }
    }

    return needWaitKeysIds;
}

Status ObjectClientImpl::PostPipelineRH2D(std::promise<AsyncResult> &promise, PiplnRh2dParam &piplnRh2dParam,
                                          GetRspPb &rsp, std::vector<std::shared_ptr<Buffer>> &buffers)
{
    PerfPoint postPoint(PerfKey::PIPLN_RH2D_CLIENT_POST_PROCESS);
    Timer postTimer;
    std::vector<std::string> failedKeys;
    auto &objectKeys = piplnRh2dParam.objectKeys;
    std::shared_ptr<H2DChunkManager> chunkManager = piplnRh2dParam.chunkManager;
    uint32_t version = piplnRh2dParam.version;
    auto config = GetClientLatencyTraceConfig();

    Status recvRc(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());

    if (recvRc.IsError()) {
        LOG(WARNING) << PIPLN_LOG_PREFIX "Pipeline failed, last error: " << recvRc.GetMsg();
    }

    if (rsp.objects_size() == 0) {
        chunkManager->CancelAll();
        const auto postUs = static_cast<uint64_t>(postTimer.ElapsedMicroSecond());
        SLOW_LOG_IF_OR_VLOG(INFO, config.processSlowerThanUs > 0 && postUs >= config.processSlowerThanUs, 1,
                            "[PIPLN RH2D] client post process done without object, objectCount: "
                                << objectKeys.size() << ", costUs: " << postUs << ", status: " << recvRc.ToString());
        buffers.clear();
        promise.set_value({ recvRc, objectKeys });
        return recvRc;
    }

    auto needWaitKeysIds = PostProcessPipelineKeys(objectKeys, rsp, piplnRh2dParam, version, failedKeys);
    {
        PerfPoint waitPoint(PerfKey::PIPLN_RH2D_CLIENT_WAIT_DONE);
        Timer waitTimer;
        Status waitRc = chunkManager->WaitAll();
        const auto waitUs = static_cast<uint64_t>(waitTimer.ElapsedMicroSecond());
        SLOW_LOG_IF_OR_VLOG(INFO, config.processSlowerThanUs > 0 && waitUs >= config.processSlowerThanUs, 1,
                            "[PIPLN RH2D] client wait done, objectCount: "
                                << objectKeys.size() << ", waitKeyCount: " << needWaitKeysIds.size()
                                << ", costUs: " << waitUs << ", status: " << waitRc.ToString());
    }
    for (auto keyIdPair : needWaitKeysIds) {
        if (!chunkManager->CheckIsRequestSuccess(keyIdPair.second)) {
            failedKeys.emplace_back(*keyIdPair.first);
        }
    }
    if (recvRc.IsOk() && failedKeys.size()) {
        recvRc = Status(K_RUNTIME_ERROR, std::to_string(failedKeys.size()) + " keys failed");
    }
    const auto postUs = static_cast<uint64_t>(postTimer.ElapsedMicroSecond());
    SLOW_LOG_IF_OR_VLOG(INFO, config.processSlowerThanUs > 0 && postUs >= config.processSlowerThanUs, 1,
                        "[PIPLN RH2D] client post process done, objectCount: "
                            << objectKeys.size() << ", rspObjectCount: " << rsp.objects_size() << ", failedCount: "
                            << failedKeys.size() << ", costUs: " << postUs << ", status: " << recvRc.ToString());
    buffers = std::move(piplnRh2dParam.buffers);
    promise.set_value({ recvRc, failedKeys });
    return recvRc;
}

#else
Status ObjectClientImpl::PostPipelineRH2D(std::promise<AsyncResult> &promise, PiplnRh2dParam &piplnRh2dParam,
                                          GetRspPb &rsp, std::vector<std::shared_ptr<Buffer>> &buffers)
{
    (void)promise;
    (void)piplnRh2dParam;
    (void)rsp;
    (void)buffers;
    return Status::OK();
}
#endif

#if defined(BUILD_PIPLN_H2D) && defined(USE_URMA)
namespace {
struct DirectPipelineItem {
    size_t index = 0;
    std::string key;
    uint64_t size = 0;
    uint64_t reqId = 0;
    uint8_t *buffer = nullptr;
    size_t replicaIndex = 0;
    bool registered = false;
    bool pipeline = false;
    bool completed = false;
    bool finalFailed = false;
    bool hasFallbackPayload = false;
    Status attemptStatus = Status(K_NOT_READY, "Client direct RH2D attempt is not started");
    std::vector<HostPort> replicas;
    std::shared_ptr<UrmaManager::BufferHandle> handle;
    UrmaRemoteAddrPb remoteAddr;
    std::shared_ptr<UrmaManager::BufferHandle> fallbackHandle;
    uint8_t *fallbackBuffer = nullptr;
};

struct DirectBatchGetTask {
    std::unique_ptr<client::DataPlaneManager::DataPlaneLease> endpointLease;
    std::shared_ptr<client::WorkerRpcClient> rpcClient;
    BatchGetObjectRemoteReqPb request;
    BatchGetObjectRemoteRspPb response;
    std::vector<RpcMessage> payloads;
    Status status = Status(K_NOT_READY, "Direct batch-get RPC is not started");
};

void RecordFirstFailure(const Status &status, Status &firstFailure)
{
    if (status.IsError() && firstFailure.IsOk()) {
        firstFailure = status;
    }
}

void AppendFailedKey(const std::string &key, std::vector<std::string> &failedKeys)
{
    failedKeys.emplace_back(key);
}

Status ParseDirectReplicas(const master::ObjectLocationInfoPb &location, std::vector<HostPort> &replicas)
{
    for (const auto &address : location.object_locations()) {
        HostPort worker;
        Status rc = worker.ParseString(address);
        if (rc.IsOk()) {
            replicas.emplace_back(std::move(worker));
        } else {
            LOG(WARNING) << PIPLN_LOG_PREFIX "Ignore invalid replica address: " << address
                         << ", status=" << rc.ToString();
        }
    }
    CHECK_FAIL_RETURN_STATUS(!replicas.empty(), K_NOT_FOUND, "No valid object replica address");
    return Status::OK();
}

Status BuildDirectPipelineItem(size_t index, const std::string &key, const Blob &devBlob,
                               const client::ObjectMetadataItem &metadata, DirectPipelineItem &item)
{
    RETURN_IF_NOT_OK(metadata.status);
    CHECK_FAIL_RETURN_STATUS(metadata.location.object_locations_size() > 0, K_NOT_FOUND,
                             "Object has no replica location");
    item.index = index;
    item.key = key;
    item.size = metadata.location.object_size();
    CHECK_FAIL_RETURN_STATUS(item.size > 0, K_INVALID, "Object size is zero");
    CHECK_FAIL_RETURN_STATUS(devBlob.pointer != nullptr && devBlob.size >= item.size, K_INVALID,
                             "Device blob is smaller than object");
    RETURN_IF_NOT_OK(ParseDirectReplicas(metadata.location, item.replicas));
    RETURN_IF_NOT_OK(UrmaManager::Instance().GetMemoryBufferHandle(item.handle, item.size));
    uint64_t bufferSize = 0;
    RETURN_IF_NOT_OK(UrmaManager::Instance().GetMemoryBufferInfo(item.handle, item.buffer, bufferSize,
                                                                 item.remoteAddr));
    CHECK_FAIL_RETURN_STATUS(bufferSize >= item.size, K_RUNTIME_ERROR, "Receive buffer is smaller than object");
    return Status::OK();
}

bool IsPendingDirectMetadata(const Status &status)
{
    switch (status.GetCode()) {
        case K_NOT_FOUND:
        case K_NOT_READY:
        case K_TRY_AGAIN:
            return true;
        default:
            return false;
    }
}

bool HasPendingDirectMetadata(const std::vector<bool> &metadataDone)
{
    return std::any_of(metadataDone.begin(), metadataDone.end(), [](bool done) { return !done; });
}

void BuildPendingDirectMetadata(const std::vector<std::string> &keys, const std::vector<bool> &metadataDone,
                                std::vector<std::string> &pendingKeys, std::vector<size_t> &pendingIndexes)
{
    pendingKeys.clear();
    pendingIndexes.clear();
    pendingKeys.reserve(keys.size());
    pendingIndexes.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        if (!metadataDone[i]) {
            pendingKeys.emplace_back(keys[i]);
            pendingIndexes.emplace_back(i);
        }
    }
}

Status CollectDirectPipelineItems(const std::vector<std::string> &keys, const std::vector<Blob> &devBlob,
                                  const std::vector<client::ObjectMetadataItem> &metadata,
                                  const std::vector<size_t> &pendingIndexes, std::vector<bool> &metadataDone,
                                  std::vector<DirectPipelineItem> &items, std::vector<std::string> &failedKeys,
                                  Status &firstFailure)
{
    for (size_t i = 0; i < pendingIndexes.size(); ++i) {
        const size_t originalIndex = pendingIndexes[i];
        if (i >= metadata.size() || IsPendingDirectMetadata(metadata[i].status)) {
            continue;
        }
        metadataDone[originalIndex] = true;
        DirectPipelineItem item;
        Status rc = BuildDirectPipelineItem(originalIndex, keys[originalIndex], devBlob[originalIndex],
                                            metadata[i], item);
        if (rc.GetCode() == K_OUT_OF_MEMORY) {
            LOG(WARNING) << PIPLN_LOG_PREFIX "Client direct receive buffer allocation failed: key="
                         << keys[originalIndex] << ", status=" << rc.ToString();
            return rc;
        }
        if (rc.IsError()) {
            LOG(ERROR) << PIPLN_LOG_PREFIX "Prepare client direct object failed: key=" << keys[originalIndex]
                       << ", status=" << rc.ToString();
            RecordFirstFailure(rc, firstFailure);
            AppendFailedKey(keys[originalIndex], failedKeys);
            continue;
        }
        items.emplace_back(std::move(item));
    }
    return Status::OK();
}

void MarkPendingMetadataFailed(const std::vector<std::string> &keys, const std::vector<bool> &metadataDone,
                               const Status &status, std::vector<std::string> &failedKeys)
{
    size_t pendingKeyNum = 0;
    for (size_t i = 0; i < keys.size(); ++i) {
        if (!metadataDone[i]) {
            AppendFailedKey(keys[i], failedKeys);
            ++pendingKeyNum;
        }
    }
    LOG(ERROR) << PIPLN_LOG_PREFIX "Client direct metadata wait failed: pendingKeyNum=" << pendingKeyNum
               << ", status=" << status.ToString();
}

Status PrepareDirectReceiver(DirectPipelineItem &item, const HostPort &worker, H2DChunkManager &manager)
{
    if (item.size <= OsXprtPipln::ChunkTag::chunkSize2MB) {
        VLOG(1) << PIPLN_LOG_PREFIX "Use one-shot URMA write for small object: key=" << item.key
                << ", reqId=" << item.reqId << ", size=" << item.size;
        return Status::OK();
    }
    UrmaRecvTargetLease recvTarget;
    Status rc = UrmaManager::Instance().AcquireRecvTarget(
        item.handle->GetSegmentAddress(), item.handle->GetSegmentSize(), worker.ToString(), recvTarget);
    if (rc.IsOk()) {
        rc = manager.DoPiplnStep1_StartReceiver(
            item.reqId, reinterpret_cast<uint64_t>(item.buffer), item.size, recvTarget.TargetSeg(),
            recvTarget.TargetJfr(), recvTarget.TargetJetty(), -1, item.size, 0);
    }
    if (rc.IsError()) {
        LOG(WARNING) << PIPLN_LOG_PREFIX "Client direct receiver failed, fallback to one-shot URMA write: key="
                     << item.key << ", reqId=" << item.reqId << ", status=" << rc.ToString();
        return Status::OK();
    }
    item.pipeline = true;
    item.remoteAddr.set_pipeline_rh2d_req_id(item.reqId);
    return Status::OK();
}

Status PrepareDirectAttempt(DirectPipelineItem &item, const Blob &devBlob, void *stream,
                            const HostPort &worker, H2DChunkManager &manager)
{
    item.reqId = UrmaManager::Instance().GenerateReqId();
    item.registered = false;
    item.pipeline = false;
    item.hasFallbackPayload = false;
    item.fallbackHandle.reset();
    item.fallbackBuffer = nullptr;
    item.attemptStatus = Status::OK();
    item.remoteAddr.clear_pipeline_rh2d_req_id();
    OsXprtPipln::DevShmInfo devInfo{ OsXprtPipln::TargetDeviceType::CUDA, (uint32_t)-1, devBlob.pointer,
                                    static_cast<size_t>(devBlob.size), stream };
    RETURN_IF_NOT_OK(manager.AddKey(item.key, item.reqId, devInfo, static_cast<int>(item.index)));
    item.registered = true;
    return PrepareDirectReceiver(item, worker, manager);
}

void FailDirectAttempt(DirectPipelineItem &item, const Status &status, H2DChunkManager &manager)
{
    item.attemptStatus = status;
    if (item.registered) {
        manager.MarkCancelOrDone(item.reqId, false);
    }
}

bool IsDirectObjectSizeMatched(const DirectPipelineItem &item, const GetObjectRemoteRspPb &rsp)
{
    if (rsp.data_size() < 0) {
        return false;
    }
    return static_cast<uint64_t>(rsp.data_size()) == item.size;
}

bool IsValidDirectDataSource(DataTransferSource source)
{
    return source == DataTransferSource::DATA_ALREADY_TRANSFERRED || source == DataTransferSource::DATA_IN_PAYLOAD;
}

Status ValidateDirectBatchResponse(const DirectPipelineItem &item, const GetObjectRemoteRspPb &rsp)
{
    Status rc(static_cast<StatusCode>(rsp.error().error_code()), rsp.error().error_msg());
    if (rc.IsError()) {
        return rc;
    }
    if (!IsDirectObjectSizeMatched(item, rsp)) {
        return Status(K_OC_REMOTE_GET_NOT_ENOUGH, "Client direct object size changed");
    }
    if (IsValidDirectDataSource(rsp.data_source())) {
        return Status::OK();
    }
    return Status(K_RUNTIME_ERROR, "Client direct response has invalid data source");
}

Status ApplyDirectPayloadResponse(DirectPipelineItem &item, RpcMessage &payload)
{
    CHECK_FAIL_RETURN_STATUS(payload.Size() == item.size, K_RUNTIME_ERROR, "Client direct payload size mismatch");
    std::shared_ptr<UrmaManager::BufferHandle> fallbackHandle;
    uint8_t *fallbackBuffer = nullptr;
    uint64_t fallbackBufferSize = 0;
    UrmaRemoteAddrPb fallbackRemoteAddr;
    RETURN_IF_NOT_OK(UrmaManager::Instance().GetMemoryBufferHandle(fallbackHandle, item.size));
    RETURN_IF_NOT_OK(UrmaManager::Instance().GetMemoryBufferInfo(fallbackHandle, fallbackBuffer, fallbackBufferSize,
                                                                 fallbackRemoteAddr));
    CHECK_FAIL_RETURN_STATUS(fallbackBufferSize >= item.size, K_RUNTIME_ERROR,
                             "Client direct fallback buffer is smaller than object");
    std::memcpy(fallbackBuffer, payload.Data(), item.size);
    item.fallbackHandle = std::move(fallbackHandle);
    item.fallbackBuffer = fallbackBuffer;
    item.hasFallbackPayload = true;
    return Status::OK();
}

Status ApplyDirectPayloadIfNeeded(DirectPipelineItem &item, const GetObjectRemoteRspPb &rsp,
                                  std::vector<RpcMessage> &payloads, size_t &payloadIndex)
{
    if (rsp.data_source() != DataTransferSource::DATA_IN_PAYLOAD) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(payloadIndex < payloads.size(), K_RUNTIME_ERROR,
                             "Client direct payload size mismatch");
    Status rc = ApplyDirectPayloadResponse(item, payloads[payloadIndex]);
    ++payloadIndex;
    return rc;
}

Status ApplyDirectBatchResponse(const std::vector<DirectPipelineItem *> &items,
                                const BatchGetObjectRemoteRspPb &response, std::vector<RpcMessage> &payloads,
                                H2DChunkManager &manager)
{
    CHECK_FAIL_RETURN_STATUS(response.responses_size() == static_cast<int>(items.size()), K_RUNTIME_ERROR,
                             "Client direct batch response size mismatch");
    size_t payloadIndex = 0;
    for (size_t i = 0; i < items.size(); ++i) {
        auto &item = *items[i];
        const auto &rsp = response.responses(static_cast<int>(i));
        Status rc = ValidateDirectBatchResponse(item, rsp);
        if (rc.IsOk()) {
            rc = ApplyDirectPayloadIfNeeded(item, rsp, payloads, payloadIndex);
        }
        if (rc.IsError()) {
            FailDirectAttempt(item, rc, manager);
        }
    }
    CHECK_FAIL_RETURN_STATUS(payloadIndex == payloads.size(), K_RUNTIME_ERROR,
                             "Client direct payload count mismatch");
    return Status::OK();
}

Status BuildDirectPipelineBatch(const std::shared_ptr<client::WorkerRpcClient> &rpcClient,
                                const std::vector<DirectPipelineItem *> &items,
                                DirectBatchGetTask &task)
{
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient);
    task.rpcClient = rpcClient;
    std::string instanceId;
    RETURN_IF_NOT_OK(GetLocalTransportInstanceId(instanceId));
    task.request.set_urma_instance_id(instanceId);
    for (auto *item : items) {
        auto *subReq = task.request.add_requests();
        subReq->set_object_key(item->key);
        subReq->set_data_size(item->size);
        subReq->set_read_offset(0);
        subReq->set_read_size(item->size);
        subReq->set_try_lock(true);
        *subReq->mutable_urma_info() = item->remoteAddr;
    }
    return Status::OK();
}

void ApplyDirectPipelineBatch(const std::vector<DirectPipelineItem *> &items,
                              DirectBatchGetTask &task, H2DChunkManager &manager)
{
    if (task.status.IsError()) {
        for (auto *item : items) {
            FailDirectAttempt(*item, task.status, manager);
        }
        return;
    }
    Status rc = ApplyDirectBatchResponse(items, task.response, task.payloads, manager);
    if (rc.IsError()) {
        for (auto *item : items) {
            if (item->attemptStatus.IsOk()) {
                FailDirectAttempt(*item, rc, manager);
            }
        }
    }
}

// The pool lifecycle remains rooted in ObjectClientImpl. This function borrows it synchronously and drains every
// submitted future before returning, so neither the pool nor task storage crosses this call's lifetime boundary.
Status InvokeDirectBatchGets(ThreadPool &taskPool, std::vector<DirectBatchGetTask> &tasks)
{
    if (tasks.empty()) {
        return Status::OK();
    }
    const auto traceContext = Trace::Instance().GetContext();
    const int64_t remainingUs = ApiDeadline::Instance().ApiRemainingUs();
    const auto dispatchTime = std::chrono::steady_clock::now();
    std::vector<std::future<Status>> futures;
    futures.reserve(tasks.size());
    Status submitStatus = Status::OK();
    try {
        for (auto &task : tasks) {
            auto *taskPtr = &task;
            futures.emplace_back(taskPool.Submit([taskPtr, traceContext, remainingUs, dispatchTime]() {
                try {
                    TraceGuard traceGuard = Trace::Instance().SetTraceContext(traceContext);
                    ApiDeadline::Instance().Push();
                    Raii deadlineRaii([]() { ApiDeadline::Instance().Pop(); });
                    RETURN_IF_NOT_OK(InitTimeoutsFromDispatch(remainingUs, dispatchTime));
                    RETURN_RUNTIME_ERROR_IF_NULL(taskPtr->rpcClient);
                    return taskPtr->rpcClient->InvokeBatchGetObject(taskPtr->request, taskPtr->response,
                                                                    taskPtr->payloads);
                } catch (const std::exception &error) {
                    return Status(K_RUNTIME_ERROR,
                                  std::string("Direct batch-get RPC task failed: ") + error.what());
                }
            }));
        }
    } catch (const std::exception &error) {
        submitStatus = Status(K_RUNTIME_ERROR, std::string("Submit direct batch-get RPC failed: ") + error.what());
    }
    for (size_t i = 0; i < futures.size(); ++i) {
        tasks[i].status = futures[i].get();
    }
    return submitStatus;
}

void SubmitDirectOneShotH2D(DirectPipelineItem &item, H2DChunkManager &manager)
{
    if (!item.registered || item.attemptStatus.IsError() || (item.pipeline && !item.hasFallbackPayload)) {
        return;
    }
    if (item.pipeline) {
        Status rc = manager.DoPiplnStep2_FallbackConsume(item.reqId, item.fallbackBuffer, item.size);
        if (rc.IsError()) {
            item.attemptStatus = rc;
            return;
        }
        item.pipeline = false;
        return;
    }
    OsXprtPipln::ChunkTag tag{ .reqId = item.reqId, .chunkType = OsXprtPipln::ChunkTag::lastChunkTag, .chunkId = 0 };
    OsXprtPipln::ChunkTag::SetObjectSize(tag, item.size);
    void *dataSrc = item.hasFallbackPayload ? item.fallbackBuffer : item.buffer;
    manager.DoPiplnStep2_ChunkConsume(item.reqId, reinterpret_cast<uint64_t>(dataSrc), tag, item.size);
}

bool IsRetryableDirectReplicaError(const Status &status)
{
    if (IsRetryableRpcError(status)) {
        return true;
    }
    switch (status.GetCode()) {
        case K_TRY_AGAIN:
        case K_URMA_NEED_CONNECT:
        case K_URMA_CONNECT_FAILED:
        case K_WORKER_PULL_OBJECT_NOT_FOUND:
        case K_NOT_FOUND:
            return true;
        default:
            return false;
    }
}

void FinishDirectItem(DirectPipelineItem &item, const Status &waitRc, H2DChunkManager &manager)
{
    OsXprtPipln::ReqInfo *info = item.registered ? manager.GetReqInfo(item.reqId) : nullptr;
    if (info != nullptr && info->ioStatus.IsError()) {
        item.attemptStatus = info->ioStatus;
        item.finalFailed = true;
        LOG(ERROR) << PIPLN_LOG_PREFIX "Client direct RH2D local H2D failed: key=" << item.key
                   << ", status=" << info->ioStatus.ToString();
        return;
    }
    const bool success = item.registered && item.attemptStatus.IsOk()
                         && manager.CheckIsRequestSuccess(item.reqId);
    if (success) {
        item.completed = true;
        return;
    }
    if (item.attemptStatus.IsOk()) {
        item.attemptStatus = waitRc.IsError()
                                 ? waitRc
                                 : Status(K_RUNTIME_ERROR, "Client direct RH2D request did not complete");
    }
    if (IsRetryableDirectReplicaError(item.attemptStatus)
        && item.replicaIndex + 1 < item.replicas.size()) {
        ++item.replicaIndex;
        VLOG(1) << PIPLN_LOG_PREFIX "Retry client direct RH2D on next replica: key=" << item.key
                << ", status=" << item.attemptStatus.ToString();
        return;
    }
    item.finalFailed = true;
    LOG(ERROR) << PIPLN_LOG_PREFIX "Client direct RH2D failed without replica retry: key=" << item.key
               << ", status=" << item.attemptStatus.ToString();
}

void FinishDirectRound(std::vector<DirectPipelineItem *> &active, H2DChunkManager &manager)
{
    Status waitRc = manager.KeyNum() > 0 ? manager.WaitAll() : Status::OK();
    for (auto *item : active) {
        FinishDirectItem(*item, waitRc, manager);
    }
}

void PrepareDirectPipelineBatches(client::TransportLayer &transport, const std::vector<Blob> &devBlob,
                                  void *stream, H2DChunkManager &manager,
                                  std::vector<std::vector<DirectPipelineItem *>> &batchItems,
                                  std::vector<DirectBatchGetTask> &batchTasks,
                                  const std::unordered_map<HostPort, std::vector<DirectPipelineItem *>> &groups)
{
    for (const auto &group : groups) {
        std::unique_ptr<client::DataPlaneManager::DataPlaneLease> endpointLease;
        Status endpointRc = transport.AcquireDirectUbEndpointLease(group.first, endpointLease);
        std::vector<DirectPipelineItem *> ready;
        for (auto *item : group.second) {
            item->registered = false;
            item->pipeline = false;
            item->hasFallbackPayload = false;
            item->attemptStatus = Status(K_NOT_READY, "Client direct attempt is not prepared");
            Status rc = endpointRc.IsOk()
                            ? PrepareDirectAttempt(*item, devBlob[item->index], stream, group.first, manager)
                            : endpointRc;
            if (rc.IsError()) {
                item->attemptStatus = rc;
            } else {
                ready.emplace_back(item);
            }
        }
        if (ready.empty()) {
            continue;
        }
        DirectBatchGetTask task;
        task.endpointLease = std::move(endpointLease);
        task.rpcClient = task.endpointLease->GetRpcClient();
        Status rc = BuildDirectPipelineBatch(task.rpcClient, ready, task);
        if (rc.IsError()) {
            for (auto *item : ready) {
                FailDirectAttempt(*item, rc, manager);
            }
            continue;
        }
        batchItems.emplace_back(std::move(ready));
        batchTasks.emplace_back(std::move(task));
    }
}

Status ApplyDirectPipelineResults(std::vector<DirectPipelineItem *> &active, ThreadPool &rpcPool,
                                  std::vector<std::vector<DirectPipelineItem *>> &batchItems,
                                  std::vector<DirectBatchGetTask> &batchTasks, H2DChunkManager &manager)
{
    Status submitRc = InvokeDirectBatchGets(rpcPool, batchTasks);
    if (submitRc.IsError()) {
        for (auto &itemsInBatch : batchItems) {
            for (auto *item : itemsInBatch) {
                FailDirectAttempt(*item, submitRc, manager);
            }
        }
    } else {
        for (size_t i = 0; i < batchTasks.size(); ++i) {
            ApplyDirectPipelineBatch(batchItems[i], batchTasks[i], manager);
        }
    }
    for (auto *item : active) {
        SubmitDirectOneShotH2D(*item, manager);
    }
    FinishDirectRound(active, manager);
    return Status::OK();
}

Status RunDirectPipelineRound(client::TransportLayer &transport, ThreadPool &rpcPool,
                              const std::vector<Blob> &devBlob, void *stream,
                              std::vector<DirectPipelineItem> &items)
{
    H2DChunkManager manager(true);
    std::vector<DirectPipelineItem *> active;
    std::unordered_map<HostPort, std::vector<DirectPipelineItem *>> groups;
    for (auto &item : items) {
        if (!item.completed && !item.finalFailed) {
            active.emplace_back(&item);
            groups[item.replicas[item.replicaIndex]].emplace_back(&item);
        }
    }
    std::vector<std::vector<DirectPipelineItem *>> batchItems;
    std::vector<DirectBatchGetTask> batchTasks;
    batchItems.reserve(groups.size());
    batchTasks.reserve(groups.size());
    PrepareDirectPipelineBatches(transport, devBlob, stream, manager, batchItems, batchTasks, groups);
    return ApplyDirectPipelineResults(active, rpcPool, batchItems, batchTasks, manager);
}

bool HasPendingDirectItems(const std::vector<DirectPipelineItem> &items)
{
    return std::any_of(items.begin(), items.end(), [](const DirectPipelineItem &item) {
        return !item.completed && !item.finalFailed;
    });
}

template <typename BuildRequest>
Status ResolveAndTransferDirectItems(client::TransportLayer &transport,
                                     ThreadPool &rpcPool,
                                     const std::vector<std::string> &objectKeys,
                                     const std::vector<Blob> &devBlob, void *h2dStream,
                                     std::vector<DirectPipelineItem> &items,
                                     std::vector<std::string> &failedKeys, Status &waitStatus,
                                     Status &firstFailure, BuildRequest &&buildRequest)
{
    std::vector<bool> metadataDone(objectKeys.size(), false);
    client::DeadlineRetry retry;
    int64_t backoffMs = 1;
    while (HasPendingDirectMetadata(metadataDone)) {
        std::vector<std::string> pendingKeys;
        std::vector<size_t> pendingIndexes;
        BuildPendingDirectMetadata(objectKeys, metadataDone, pendingKeys, pendingIndexes);
        client::ObjectReadRequest request;
        std::vector<Status> itemStatuses(pendingKeys.size(), Status(K_NOT_READY, "Metadata is not resolved"));
        buildRequest(pendingKeys, request, itemStatuses);
        std::vector<client::ObjectMetadataItem> metadata;
        RETURN_IF_NOT_OK(transport.ResolveMetadata(request, metadata));
        metadata.resize(pendingKeys.size());
        RETURN_IF_NOT_OK(CollectDirectPipelineItems(objectKeys, devBlob, metadata, pendingIndexes,
                                                    metadataDone, items, failedKeys, firstFailure));
        while (HasPendingDirectItems(items)) {
            RETURN_IF_NOT_OK(retry.CheckDeadline());
            RETURN_IF_NOT_OK(RunDirectPipelineRound(transport, rpcPool, devBlob, h2dStream, items));
        }
        if (!HasPendingDirectMetadata(metadataDone)) {
            break;
        }
        waitStatus = retry.Backoff(backoffMs);
        if (waitStatus.IsError()) {
            RecordFirstFailure(waitStatus, firstFailure);
            MarkPendingMetadataFailed(objectKeys, metadataDone, waitStatus, failedKeys);
            break;
        }
    }
    return Status::OK();
}

template <typename MaterializeItem>
void MaterializeDirectItems(std::vector<DirectPipelineItem> &items,
                            std::vector<std::shared_ptr<Buffer>> &buffers,
                            std::vector<std::string> &failedKeys, Status &firstFailure,
                            MaterializeItem &&materializeItem)
{
    for (auto &item : items) {
        if (!item.completed) {
            if (item.attemptStatus.IsError()) {
                RecordFirstFailure(item.attemptStatus, firstFailure);
            } else {
                RecordFirstFailure(Status(K_RUNTIME_ERROR, "Client direct RH2D item did not complete"),
                                   firstFailure);
            }
            AppendFailedKey(item.key, failedKeys);
            continue;
        }
        client::ObjectReadItemResult result;
        result.objectKey = item.key;
        result.status = Status::OK();
        result.data.response.set_data_size(item.size);
        result.data.response.set_data_source(DataTransferSource::DATA_ALREADY_TRANSFERRED);
        result.data.externalData = item.hasFallbackPayload ? item.fallbackBuffer : item.buffer;
        result.data.externalSize = item.size;
        result.data.externalOwner = std::make_shared<DirectReceiveBufferOwner>(item.handle, item.fallbackHandle);
        result.data.kind = AccessTransportKind::UB;
        Status rc = materializeItem(item.key, result, buffers[item.index]);
        if (rc.IsError()) {
            LOG(ERROR) << PIPLN_LOG_PREFIX "Materialize client direct RH2D result failed: key=" << item.key
                       << ", status=" << rc.ToString();
            RecordFirstFailure(rc, firstFailure);
            AppendFailedKey(item.key, failedKeys);
        }
    }
}

}  // namespace

Status ObjectClientImpl::RunClientDirectPipelineRH2D(const std::vector<std::string> &objectKeys,
                                                     const std::vector<Blob> &devBlob,
                                                     std::vector<std::shared_ptr<Buffer>> &buffers,
                                                     void *h2dStream,
                                                     std::vector<std::string> &failedKeys)
{
    RETURN_RUNTIME_ERROR_IF_NULL(transportLayer_);
    RETURN_RUNTIME_ERROR_IF_NULL(asyncGetRPCPool_);
    std::vector<DirectPipelineItem> items;
    items.reserve(objectKeys.size());
    Status waitStatus = Status::OK();
    Status firstFailure = Status::OK();
    auto buildRequest = [this](const std::vector<std::string> &keys, client::ObjectReadRequest &request,
                               std::vector<Status> &statuses) {
        boundMode_->BuildClientDirectRH2DReadRequest(keys, request, statuses, requestTimeoutMs_, true);
    };
    Status rc = ResolveAndTransferDirectItems(*transportLayer_, *asyncGetRPCPool_, objectKeys, devBlob,
                                              h2dStream, items, failedKeys, waitStatus, firstFailure, buildRequest);
    if (rc.IsError()) {
        for (const auto &key : objectKeys) {
            AppendFailedKey(key, failedKeys);
        }
        return rc;
    }
    auto materializeItem = [this](const std::string &key, client::ObjectReadItemResult &item,
                                  std::shared_ptr<Buffer> &buffer) {
        return MaterializeTransportItem(key, item, buffer);
    };
    MaterializeDirectItems(items, buffers, failedKeys, firstFailure, materializeItem);
    if (waitStatus.IsError()) {
        return waitStatus;
    }
    if (failedKeys.empty()) {
        return Status::OK();
    }
    return firstFailure.IsError() ? firstFailure
                                  : Status(K_RUNTIME_ERROR, std::to_string(failedKeys.size()) + " keys failed");
}

#elif defined(BUILD_PIPLN_H2D)
Status ObjectClientImpl::RunClientDirectPipelineRH2D(const std::vector<std::string> &objectKeys,
                                                     const std::vector<Blob> &devBlob,
                                                     std::vector<std::shared_ptr<Buffer>> &buffers,
                                                     void *h2dStream,
                                                     std::vector<std::string> &failedKeys)
{
    (void)devBlob;
    (void)buffers;
    (void)h2dStream;
    failedKeys = objectKeys;
    return Status(K_NOT_SUPPORTED, "Client direct pipeline H2D requires USE_URMA");
}
#endif

Status ObjectClientImpl::GetWithLatch(const std::vector<std::string> &objectKeys, std::vector<std::string> &vals,
                                      int64_t subTimeoutMs, std::vector<Optional<Buffer>> &buffers, size_t &dataSize)
{
    vals.clear();
    Status rc = Get(objectKeys, subTimeoutMs, buffers);
    for (auto &buffer : buffers) {
        if (buffer) {
            // Use the SDK-internal helper so the read-and-copy works whether the
            // shm buffer has a metadata-header lock or not (oc_metadata_header=false
            // → DisabledLock → no latch needed for safe reads).
            RETURN_IF_NOT_OK(buffer->CopyDataWithRLatch([&] {
                const void *data = buffer->ImmutableData();
                if (data == nullptr || buffer->GetSize() == 0) {
                    vals.emplace_back();
                    return Status::OK();
                }
                vals.emplace_back(reinterpret_cast<const char *>(data), buffer->GetSize());
                dataSize += buffer->GetSize();
                return Status::OK();
            }));
        } else {
            vals.emplace_back();
        }
    }
    return rc;
}

void ObjectClientImpl::BuildTransportReadRequest(const std::vector<std::string> &objectKeys,
                                                 client::ObjectReadRequest &request,
                                                 std::vector<Status> &itemStatuses, int64_t subTimeoutMs,
                                                 bool queryL2Cache)
{
    auto context = std::make_shared<client::TransportReadContext>();
    context->requestContext.clientId = GetClientId();
    const auto token = std::atomic_load(&transportToken_);
    if (token != nullptr && !token->Empty()) {
        context->requestContext.token.assign(token->GetData(), token->GetSize());
    }
    const auto &requestTenantId = GetRequestContext()->tenantId;
    context->requestContext.tenantId = requestTenantId.empty() ? tenantId_ : requestTenantId;
    context->subTimeoutMs = subTimeoutMs;
    context->queryL2Cache = queryL2Cache;
    request.context = std::move(context);

    auto routing = std::atomic_load(&routing_);
    if (routing == nullptr) {
        std::fill(itemStatuses.begin(), itemStatuses.end(), Status(K_NOT_READY, "Object route is not ready"));
        LOG_EVERY_N(ERROR, TRANSPORT_DIAG_LOG_RATE)
            << "[TransportGet][Route] Route is not ready, key count: " << objectKeys.size();
        return;
    }
    std::unordered_map<HostPort, std::vector<std::string>> groupedKeys;
    Status routeStatus =
        routing->SelectWorkers(objectKeys, client::DataPlacementPolicy::PREFERRED_META_OWNER, groupedKeys);
    if (routeStatus.IsError()) {
        std::fill(itemStatuses.begin(), itemStatuses.end(), routeStatus);
        LOG(ERROR) << "[TransportGet][Route] Route selection failed, key count: " << objectKeys.size()
                   << ", status: " << routeStatus.ToString();
        return;
    }
    std::unordered_map<std::string, HostPort> metaOwners;
    metaOwners.reserve(objectKeys.size());
    for (const auto &group : groupedKeys) {
        for (const auto &key : group.second) {
            metaOwners.emplace(key, group.first);
        }
    }
    for (size_t i = 0; i < objectKeys.size(); ++i) {
        auto owner = metaOwners.find(objectKeys[i]);
        if (owner == metaOwners.end()) {
            itemStatuses[i] = Status(K_RUNTIME_ERROR, "Batch route result is incomplete");
            LOG_EVERY_N(ERROR, TRANSPORT_DIAG_LOG_RATE)
                << "[TransportGet][Route] Route result is incomplete, key: " << objectKeys[i]
                << ", request index: " << i << ", status: " << itemStatuses[i].ToString();
            continue;
        }
        itemStatuses[i] = Status::OK();
        request.items.push_back({ i, objectKeys[i], owner->second });
    }
    const size_t routed = request.items.size();
    const size_t failed = objectKeys.size() >= routed ? objectKeys.size() - routed : 0;
    VLOG(1) << "[TransportGet][Route] Route selection completed, key count: " << objectKeys.size()
            << ", routed: " << routed << ", failed: " << failed << ", meta owner count: " << groupedKeys.size();
}

Status ObjectClientImpl::BuildTransportGetResponse(
    client::ObjectReadItemResult &item, GetRspPb &response,
    std::unordered_map<std::string, std::shared_ptr<ObjectBufferInfo>> &ubBufferInfos, uint64_t &payloadSize)
{
    auto &data = item.data;
    const uint64_t dataSize = data.externalOwner != nullptr
                                  ? data.externalSize
                                  : static_cast<uint64_t>(std::max<int64_t>(data.response.data_size(), 0));
    payloadSize = 0;
    auto *payloadInfo = response.add_payload_info();
    payloadInfo->set_object_key(item.objectKey);
    payloadInfo->set_object_index(0);
    payloadInfo->set_data_size(static_cast<int64_t>(dataSize));
    if (data.externalOwner != nullptr) {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            data.response.data_size() >= 0
                && static_cast<uint64_t>(data.response.data_size()) == data.externalSize,
            K_RUNTIME_ERROR, "Invalid object data response");
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(data.externalData != nullptr || dataSize == 0, K_RUNTIME_ERROR,
                                             "Invalid object data response");
        FullParam param;
        auto bufferInfo = boundMode_->MakeObjectBufferInfo(item.objectKey,
                                                           const_cast<uint8_t *>(data.externalData), dataSize, 0,
                                                            param, false, 0);
        bufferInfo->ubGetBufferHandle = data.externalOwner;
        ubBufferInfos.emplace(item.objectKey, std::move(bufferInfo));
        payloadInfo->add_part_index(0);
        data.rpcPayloads.emplace_back();
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(data.externalData == nullptr && data.externalSize == 0, K_RUNTIME_ERROR,
                                         "Invalid object data response");
    for (size_t i = 0; i < data.rpcPayloads.size(); ++i) {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(payloadSize <= UINT64_MAX - data.rpcPayloads[i].Size(), K_RUNTIME_ERROR,
                                             "Invalid object data response");
        payloadSize += data.rpcPayloads[i].Size();
        payloadInfo->add_part_index(static_cast<uint32_t>(i));
    }
    LOG_IF(ERROR, payloadSize != dataSize)
        << "[TransportGet][Materialize] RPC payload size mismatch, key=" << item.objectKey
        << ", responseDataSize=" << data.response.data_size() << ", payloadSize=" << payloadSize
        << ", payloadCount=" << data.rpcPayloads.size()
        << ", dataSource=" << static_cast<int>(data.response.data_source());
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(payloadSize == dataSize, K_RUNTIME_ERROR, "Invalid object data response");
    return Status::OK();
}

Status ObjectClientImpl::MaterializeTransportItem(const std::string &objectKey, client::ObjectReadItemResult &item,
                                                  std::shared_ptr<Buffer> &buffer)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(item.objectKey == objectKey, K_RUNTIME_ERROR,
                                         "Invalid object data response");
    auto &data = item.data;
    if (data.externalMeta.has_value()) {
        CHECK_FAIL_RETURN_STATUS(data.externalOwner != nullptr, K_RUNTIME_ERROR,
                                 "SHM object data owner is missing");
        CHECK_FAIL_RETURN_STATUS(data.externalData != nullptr || data.externalSize == 0, K_RUNTIME_ERROR,
                                 "SHM object data pointer is missing");
        const auto &meta = *data.externalMeta;
        FullParam param;
        param.writeMode = meta.mode.GetWriteMode();
        param.consistencyType = meta.mode.GetConsistencyType();
        param.cacheType = meta.mode.GetCacheType();
        // The routed owner checks the target session generation, so the legacy initial-Worker version is unused.
        auto bufferInfo = boundMode_->MakeObjectBufferInfo(item.objectKey, const_cast<uint8_t *>(data.externalData),
                                                           data.externalSize, meta.metadataSize, param, meta.isSeal, 0,
                                                            meta.shmId);
        bufferInfo->workerAddr = meta.workerAddr;
        bufferInfo->receiveBufferOwner = std::move(data.externalOwner);
        bufferInfo->sessionLockId = meta.lockId;
        bufferInfo->useSessionLockId = true;
        return Buffer::CreateBuffer(std::move(bufferInfo), shared_from_this(), buffer);
    }

    GetRspPb response;
    std::unordered_map<std::string, std::shared_ptr<ObjectBufferInfo>> ubBufferInfos;
    uint64_t payloadSize = 0;
    RETURN_IF_NOT_OK(BuildTransportGetResponse(item, response, ubBufferInfos, payloadSize));
    const uint64_t dataSize = static_cast<uint64_t>(response.payload_info(0).data_size());
    VLOG(1) << "[TransportGet][Materialize] Materialize object, key: " << objectKey
            << ", transport: " << AccessTransportTracker::KindToName(data.kind) << ", data size: " << dataSize
            << ", payload size: " << payloadSize << ", payload count: " << data.rpcPayloads.size()
            << ", external size: " << data.externalSize
            << ", data source: " << static_cast<int>(data.response.data_source());
    std::vector<std::shared_ptr<Buffer>> itemBuffers(1);
    std::vector<std::string> failedKeys;
    RETURN_IF_NOT_OK(boundMode_->ProcessGetResponse({ item.objectKey }, {}, response, 0, data.rpcPayloads, itemBuffers,
                                                    failedKeys, ubBufferInfos));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(failedKeys.empty() && itemBuffers.front() != nullptr, K_NOT_FOUND,
                                         "Cannot get objects from worker");
    buffer = std::move(itemBuffers.front());
    return Status::OK();
}

Status ObjectClientImpl::ApplyTransportReadResult(const std::vector<std::string> &objectKeys,
                                                  const client::ObjectReadRequest &request,
                                                  client::ObjectReadResult &result, const Status &transportStatus,
                                                  std::vector<std::shared_ptr<Buffer>> &buffers,
                                                  std::vector<Status> &itemStatuses, AccessTransportKind &actualKind)
{
    // a fully-failed Get still reflects the real transport instead of the SHM default from Reset().
    actualKind = static_cast<AccessTransportKind>(
        std::max(static_cast<uint8_t>(actualKind), static_cast<uint8_t>(result.actualKind)));
    std::vector<bool> returned(objectKeys.size(), false);
    for (auto &item : result.items) {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(item.requestIndex < objectKeys.size(), K_RUNTIME_ERROR,
                                             "Invalid response while getting objects");
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!returned[item.requestIndex], K_RUNTIME_ERROR,
                                             "Invalid response while getting objects");
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(item.objectKey == objectKeys[item.requestIndex], K_RUNTIME_ERROR,
                                             "Invalid response while getting objects");
        returned[item.requestIndex] = true;
        itemStatuses[item.requestIndex] = item.status;
        if (item.status.IsOk()) {
            itemStatuses[item.requestIndex] =
                MaterializeTransportItem(item.objectKey, item, buffers[item.requestIndex]);
            if (itemStatuses[item.requestIndex].IsOk()) {
                actualKind = static_cast<AccessTransportKind>(std::max(
                    static_cast<uint8_t>(actualKind), static_cast<uint8_t>(item.data.kind)));
            }
        }
    }
    for (const auto &item : request.items) {
        if (!returned[item.requestIndex]) {
            itemStatuses[item.requestIndex] = transportStatus.IsError()
                                                  ? transportStatus
                                                  : Status(K_RUNTIME_ERROR, "Cannot get objects from worker");
            LOG_EVERY_N(ERROR, TRANSPORT_DIAG_LOG_RATE)
                << "[TransportGet][Result] Object result is missing, key: " << item.objectKey
                << ", request index: " << item.requestIndex
                << ", status: " << itemStatuses[item.requestIndex].ToString();
        }
    }
    if (VLOG_IS_ON(1)) {
        const auto succeeded = std::count_if(itemStatuses.begin(), itemStatuses.end(),
                                             [](const Status &status) { return status.IsOk(); });
        VLOG(1) << "[TransportGet][Result] Apply result completed, requested: " << objectKeys.size()
                << ", routed: " << request.items.size() << ", returned: "
                << std::count(returned.begin(), returned.end(), true) << ", succeeded: " << succeeded
                << ", failed: " << itemStatuses.size() - succeeded
                << ", actual transport: " << AccessTransportTracker::KindToName(actualKind);
    }
    return Status::OK();
}

Status ObjectClientImpl::FinishTransportRead(const std::vector<Status> &itemStatuses,
                                             AccessTransportKind actualKind, const Status &transportStatus)
{
    const bool anyOk = std::any_of(itemStatuses.begin(), itemStatuses.end(),
                                   [](const Status &status) { return status.IsOk(); });
    // Record the transport whenever the transport layer actually carried data over a non-SHM
    // medium, or at least one item succeeded — covers the fully-failed Get case where
    // ApplyTransportReadResult still seeded actualKind from result.actualKind.
    if (anyOk || actualKind != AccessTransportKind::SHM) {
        AccessTransportTracker::Record(actualKind);
    }
    if (anyOk) {
        return Status::OK();
    }
    for (const auto &status : itemStatuses) {
        if (status.IsError()) {
            return status;
        }
    }
    return transportStatus.IsError() ? transportStatus : Status(K_RUNTIME_ERROR, "Failed to get objects");
}

Status ObjectClientImpl::ReadTransportRound(const std::vector<std::string> &objectKeys, bool traceEnabled,
                                            int64_t subTimeoutMs, bool queryL2Cache,
                                            std::vector<std::shared_ptr<Buffer>> &buffers,
                                            std::vector<Status> &itemStatuses, AccessTransportKind &actualKind,
                                            Status &transportStatus)
{
    client::ObjectReadRequest request;
    request.traceEnabled = traceEnabled;
    AddLatencyTickIfEnabled(traceEnabled, LatencyTickKey::CLIENT_DIRECT_ROUTE_START);
    BuildTransportReadRequest(objectKeys, request, itemStatuses, subTimeoutMs, queryL2Cache);
    AddLatencyTickIfEnabled(traceEnabled, LatencyTickKey::CLIENT_DIRECT_ROUTE_END);
    client::ObjectReadResult result;
    transportStatus = request.items.empty() ? Status(K_NOT_READY, "No object route is available")
                                            : transportLayer_->Get(request, result);
    AddLatencyTickIfEnabled(traceEnabled, LatencyTickKey::CLIENT_DIRECT_MATERIALIZE_START);
    Status applyStatus =
        ApplyTransportReadResult(objectKeys, request, result, transportStatus, buffers, itemStatuses, actualKind);
    AddLatencyTickIfEnabled(traceEnabled, LatencyTickKey::CLIENT_DIRECT_MATERIALIZE_END);
    return applyStatus;
}

Status ObjectClientImpl::GetFromTransportLayer(const std::vector<std::string> &objectKeys,
                                               std::vector<std::shared_ptr<Buffer>> &buffers, bool traceEnabled,
                                               int64_t subTimeoutMs, bool queryL2Cache)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(transportLayer_ != nullptr, K_NOT_READY, "Object service is not ready");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(objectKeys.size() == buffers.size(), K_RUNTIME_ERROR,
                                         "Failed to prepare object Get request");
    std::vector<Status> itemStatuses(objectKeys.size(), Status(K_NOT_READY, "Object Get has not completed"));
    AccessTransportKind actualKind = AccessTransportKind::SHM;
    Status transportStatus(K_NOT_READY, "No object route is available");
    std::vector<TransportReadRetryState> retryStates;
    client::DeadlineRetry retry;
    bool refreshRequested = false;

    RETURN_IF_NOT_OK(ReadTransportRound(objectKeys, traceEnabled, subTimeoutMs, queryL2Cache, buffers, itemStatuses,
                                        actualKind, transportStatus));
    CollectInitialTransportReadRetryStates(itemStatuses, retryStates);

    while (ApiDeadline::Instance().ApiRemainingUs() > 0) {
        auto retryIndexes = BuildNextTransportReadRetry(retryStates);
        if (retryIndexes.empty()) {
            break;
        }
        auto routing = std::atomic_load(&routing_);
        Status waitStatus = PrepareTransportReadRetry(routing, retryIndexes, retryStates, retry, refreshRequested);
        if (waitStatus.IsError()) {
            transportStatus = waitStatus;
            ApplyTransportReadRetryWaitFailure(retryIndexes, retryStates, waitStatus, itemStatuses);
            break;
        }

        std::vector<std::string> retryKeys;
        retryKeys.reserve(retryIndexes.size());
        for (auto stateIndex : retryIndexes) {
            retryKeys.emplace_back(objectKeys[retryStates[stateIndex].outputIndex]);
        }
        TransportReadRoundResult roundResult;
        roundResult.buffers.resize(retryKeys.size());
        roundResult.statuses.resize(retryKeys.size(), Status(K_NOT_READY, "Object Get has not completed"));
        RETURN_IF_NOT_OK(ReadTransportRound(retryKeys, traceEnabled, subTimeoutMs, queryL2Cache, roundResult.buffers,
                                            roundResult.statuses, actualKind, transportStatus));
        CollectRetryTransportReadRound(retryIndexes, roundResult, buffers, itemStatuses, retryStates);
    }
    ApplyTransportReadRetryBudgetFailure(retryStates, itemStatuses);
    return FinishTransportRead(itemStatuses, actualKind, transportStatus);
}

Status ObjectClientImpl::Get(const std::vector<std::string> &objectKeys, int64_t subTimeoutMs,
                             std::vector<Optional<Buffer>> &buffers, bool queryL2Cache, bool isRH2DSupported,
                             int32_t requestTimeoutMs)
{
    PerfPoint perfPoint(PerfKey::CLIENT_GET_OBJECT);
    AccessTransportTracker::Reset();
    RETURN_IF_NOT_OK(IsClientReady());
    RETURN_IF_NOT_OK(CheckValidObjectKeyVector(objectKeys));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(objectKeys.size()), K_INVALID,
                                         FormatString("The objectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    const int32_t effectiveTimeoutMs = requestTimeoutMs > 0 ? requestTimeoutMs : requestTimeoutMs_;
    ApiDeadlineGuard deadlineGuard(effectiveTimeoutMs);
    auto config = GetClientLatencyTraceConfig();
    const bool traceEnabled = ShouldCollectLatencyTrace(config);
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_GET_START);
    }
    std::vector<std::shared_ptr<Buffer>> objectBuffers(objectKeys.size());
    Status rc;
    if (!enableLocalCache_) {
        CHECK_FAIL_RETURN_STATUS(!isRH2DSupported, K_NOT_SUPPORTED,
                                 "Remote H2D is not supported when local cache is disabled");
        rc = GetFromTransportLayer(objectKeys, objectBuffers, traceEnabled, subTimeoutMs, queryL2Cache);
    } else {
        rc = boundMode_->GetFromLocalWorker(objectKeys, subTimeoutMs, objectBuffers, queryL2Cache, isRH2DSupported,
                                            requestTimeoutMs);
    }
    buffers.clear();
    for (auto &objectBuffer : objectBuffers) {
        if (objectBuffer == nullptr) {
            buffers.emplace_back();
        } else {
            buffers.emplace_back(std::move(*objectBuffer));
        }
    }
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_GET_END);
    }
    EmitClientLatencySummary(LatencyTickKey::CLIENT_GET_START, LatencyTickKey::CLIENT_GET_END);
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
    ApiDeadlineGuard deadlineGuard(requestTimeoutMs_);
    GetParam getParam{ .objectKeys = objectKeys, .subTimeoutMs = 0, .readParams = readParams };
    Status rc = boundMode_->GetBuffersFromWorker(workerApi, getParam, objectBuffers);
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

Status ObjectClientImpl::CheckValidObjectKey(const std::string &key)
{
    CHECK_FAIL_RETURN_STATUS(Validator::IsIdFormat(key), K_INVALID,
        FormatString("The key contains illegal char(s), allowed regex format: %s "
                     "or the length of key must be no more than %u. Current key: %s, length: %d.",
                     Validator::objKeyFormat, MAX_KEY_LENGTH, FormatStringForLog(key), key.size()));
    return Status::OK();
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
    ApiDeadlineGuard deadlineGuard(requestTimeoutMs_);
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

Status ObjectClientImpl::Set(const std::shared_ptr<Buffer> &buffer)
{
    AccessTransportTracker::Reset();
    RETURN_IF_NOT_OK(IsClientReady());
    ApiDeadlineGuard deadlineGuard(requestTimeoutMs_);
    GetRequestContext()->reqTimeoutDuration.InitUs(ApiDeadline::Instance().ApiRemainingUs());
    CHECK_FAIL_RETURN_STATUS(buffer != nullptr, K_INVALID, "The buffer should not be empty.");
    RETURN_IF_NOT_OK(buffer->CheckDeprecated());
    std::shared_lock<std::shared_timed_mutex> shutdownLck(shutdownMux_);
    PerfPoint perfPoint(PerfKey::CLIENT_PUT_OBJECT);
    VLOG(1) << "Start putting buffer";
    auto config = GetClientLatencyTraceConfig();
    const bool traceEnabled = ShouldCollectLatencyTrace(config);
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_SET_START);
    }
    auto rc = buffer->Publish();
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_SET_END);
    }
    EmitClientLatencySummary(LatencyTickKey::CLIENT_SET_START, LatencyTickKey::CLIENT_SET_END);
    return rc;
}

Status ObjectClientImpl::MSetRoutedBuffers(const std::vector<std::shared_ptr<Buffer>> &buffers, bool allRouted)
{
    RETURN_RUNTIME_ERROR_IF_NULL(transportLayer_);
    // Group routed bufferInfos by the worker pinned at Create time.
    std::unordered_map<HostPort, std::vector<std::shared_ptr<ObjectBufferInfo>>> grouped;
    size_t totalRouted = 0;
    for (const auto &buffer : buffers) {
        if (buffer == nullptr || buffer->bufferInfo_->dataSize == 0 || !buffer->bufferInfo_->isRoutedWrite) {
            continue;  // nullptr or MCreate NX placeholder (dataSize=0) or legacy buffer.
        }
        grouped[buffer->bufferInfo_->workerAddr].push_back(buffer->bufferInfo_);
        ++totalRouted;
    }
    if (grouped.empty()) {
        return Status::OK();
    }
    if (!allRouted) {
        // Mixed batch (should not happen in normal use): publish routed buffers one by one and let
        // the caller drive the legacy buffers through the bound-worker path below.
        Status lastRc = Status::OK();
        for (const auto &buffer : buffers) {
            if (buffer == nullptr || buffer->bufferInfo_->dataSize == 0 || !buffer->bufferInfo_->isRoutedWrite) {
                continue;
            }
            auto rc = PublishRoutedBuffer(buffer->bufferInfo_, {}, false);
            if (rc.IsError()) {
                lastRc = rc;
            }
        }
        return lastRc;
    }
    // All routed: batch the publish per worker via transportLayer_->MSet, mirroring ProcessTransportMSet.
    Status lastRc = Status::OK();
    size_t failedCount = 0;
    for (auto &entry : grouped) {
        auto rc = ProcessRoutedMSetGroup(entry.first, entry.second, failedCount);
        if (rc.IsError()) {
            lastRc = rc;
        }
    }
    // Mirror MSetThroughTransport: report OK when at least one object published successfully.
    return (failedCount < totalRouted) ? Status::OK() : lastRc;
}

Status ObjectClientImpl::ProcessRoutedMSetGroup(const HostPort &worker,
                                                const std::vector<std::shared_ptr<ObjectBufferInfo>> &infos,
                                                size_t &failedCount)
{
    const auto sourceDraining = [](const auto &info) { return info->routedWriteSourceDraining; };
    if (std::any_of(infos.begin(), infos.end(), sourceDraining)) {
        return ReplayRoutedMSetGroup(infos, failedCount);
    }
    SetRouteContext routeContext;
    RETURN_IF_NOT_OK(BuildSetRouteContext(worker, routeContext));
    const auto requestContext = BuildTransportRequestContext(routeContext);
    std::vector<std::shared_ptr<ObjectBuffer>> objBufs;
    objBufs.reserve(infos.size());
    for (const auto &info : infos) {
        std::shared_ptr<ObjectBuffer> objBuf;
        auto rc = ObjectBufferInternal::Create(info, objBuf);
        if (rc.IsError()) {
            // Reconstruct failed mid-group: nothing published for this group.
            failedCount += infos.size();
            return rc;
        }
        // The legacy Buffer owns the payload; keep the transient ObjectBuffer from freeing it.
        ObjectBufferInternal::DisownLocalMemory(*objBuf);
        objBufs.push_back(std::move(objBuf));
    }
    client::TransportSetParam setParam;
    setParam.requestContext = requestContext;
    // MSet(buffers) is the publish step after MCreate; carry the existence opt recorded on the
    // buffer at Create time (mirrors single PublishRoutedBuffer).
    setParam.existence = static_cast<ExistenceOpt>(infos.front()->existence);
    setParam.ttlSecond = infos.front()->ttlSecond;
    setParam.subTimeoutMs = requestTimeoutMs_;
    client::TransportMSetResult result;
    auto rc = transportLayer_->MSet(objBufs, setParam, result);
    if (rc.GetCode() == K_SCALE_DOWN) {
        return ReplayRoutedMSetGroup(infos, failedCount);
    }
    failedCount += rc.IsError() ? infos.size() : result.failedKeys.size();
    return rc;
}

Status ObjectClientImpl::ReplayRoutedMSetGroup(const std::vector<std::shared_ptr<ObjectBufferInfo>> &infos,
                                               size_t &failedCount)
{
    Status lastRc = Status::OK();
    const size_t failedBefore = failedCount;
    for (const auto &info : infos) {
        Status rc = ReplayRoutedBuffer(info, {}, false);
        if (rc.IsError()) {
            ++failedCount;
            lastRc = std::move(rc);
        }
    }
    return failedCount - failedBefore < infos.size() ? Status::OK() : lastRc;
}

Status ObjectClientImpl::MSet(const std::vector<std::shared_ptr<Buffer>> &buffers)
{
    AccessTransportTracker::Reset();
    CHECK_FAIL_RETURN_STATUS(!buffers.empty(), K_INVALID, "The buffer list must not be empty.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(buffers.size()), K_INVALID,
                                         FormatString("The buffer size cannot exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    RETURN_IF_NOT_OK(IsClientReady());
    ApiDeadlineGuard deadlineGuard(requestTimeoutMs_);
    GetRequestContext()->reqTimeoutDuration.InitUs(ApiDeadline::Instance().ApiRemainingUs());
    const size_t bufferCnt = buffers.size();
    std::vector<std::shared_ptr<ObjectBufferInfo>> bufferInfoList;
    bufferInfoList.reserve(bufferCnt);
    bool hasRouted = false;
    std::optional<ExistenceOpt> batchExistence;
    for (size_t i = 0; i < bufferCnt; i++) {
        auto &buffer = buffers[i];
        CHECK_FAIL_RETURN_STATUS(buffers[i] != nullptr, K_INVALID, "The buffer should not be empty.");
        RETURN_IF_NOT_OK(buffer->CheckDeprecated());
        // MCreate NX placeholder: key already exists, dataSize=0, skip publishing
        if (buffer->bufferInfo_->dataSize == 0) {
            continue;
        }
        const auto existence = static_cast<ExistenceOpt>(buffer->bufferInfo_->existence);
        CHECK_FAIL_RETURN_STATUS(existence == ExistenceOpt::NONE || existence == ExistenceOpt::NX, K_INVALID,
                                 "The buffer has an invalid existence option.");
        CHECK_FAIL_RETURN_STATUS(!batchExistence || *batchExistence == existence, K_INVALID,
                                 "Buffers in one MSet must use the same existence option.");
        batchExistence = existence;
        // Routed (lc=false two-step Create) buffers are published through the transport layer below.
        CHECK_FAIL_RETURN_STATUS(!buffer->bufferInfo_->isSeal, K_OC_ALREADY_SEALED, "Client object is already sealed");
        RETURN_IF_NOT_OK(buffer->CopyPageableDataToShm());
        if (buffer->bufferInfo_->isRoutedWrite) {
            hasRouted = true;
            continue;
        }
        bufferInfoList.push_back(buffer->bufferInfo_);
    }
    // Routed buffers: publish on the worker pinned at Create time. When bufferInfoList is empty
    // every non-placeholder buffer is routed (all-routed batch); otherwise it is a mixed batch.
    Status routedRc = Status::OK();
    if (hasRouted) {
        routedRc = MSetRoutedBuffers(buffers, bufferInfoList.empty());
    }
    if (bufferInfoList.empty()) {
        return routedRc;  // all-routed/all-placeholder, or legacy publish fully handled above
    }
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    const uint32_t ttl = bufferInfoList.front()->ttlSecond;
    // MCreate's existence check is only an early filter. MultiPublish must carry the original option so the worker and
    // metadata owner can make the final atomic NX decision.
    PublishParam publishParam{ .isReplica = false, .existence = *batchExistence, .ttlSecond = ttl };
    MultiPublishRspPb rsp;
    RETURN_IF_NOT_OK(workerApi->MultiPublish(bufferInfoList, publishParam, rsp));
    auto rc = HandleShmRefCountAfterMultiPublish(buffers, rsp);
    return rc.IsError() ? rc : routedRc;
}

Status ObjectClientImpl::Set(const std::string &key, const StringView &val, const SetParam &setParam)
{
    return Set(key, val, setParam, 0);
}

Status ObjectClientImpl::Set(const std::string &key, const StringView &val, const SetParam &setParam,
                             int32_t requestTimeoutMs)
{
    AccessTransportTracker::Reset();
    RETURN_IF_NOT_OK(IsClientReady());
    RETURN_IF_NOT_OK(CheckValidObjectKey(key));
    FullParam param;
    param.writeMode = setParam.writeMode;
    param.consistencyType = ConsistencyType::CAUSAL;
    param.cacheType = setParam.cacheType;
    return Put(key, reinterpret_cast<const uint8_t *>(val.data()), val.size(), param, {}, setParam.ttlSecond,
               static_cast<int>(setParam.existence), requestTimeoutMs);
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
    ApiDeadlineGuard deadlineGuard(requestTimeoutMs_);
    auto rc = MultiCreate(keys, sizes, param, skipCheckExistence, buffers, exist);
    return rc;
}

Status ObjectClientImpl::MemoryCopyTransportMSetBuffers(
    const MSetRouteGroup &group, const std::vector<std::shared_ptr<ObjectBuffer>> &buffers, uint64_t dataSizeSum)
{
    CHECK_FAIL_RETURN_STATUS(group.values.size() == buffers.size(), K_RUNTIME_ERROR,
                             "MSet transport buffer count mismatch");
    RETURN_IF_NOT_OK(ApiDeadline::Instance().CheckApiDeadline());
    auto memoryCopy = [&](size_t start, size_t end) {
        for (size_t i = start; i < end; ++i) {
            RETURN_RUNTIME_ERROR_IF_NULL(buffers[i]);
            const auto &value = group.values[i];
            const int64_t bufferSize = buffers[i]->GetSize();
            auto *bufferData = static_cast<uint8_t *>(buffers[i]->MutableData());
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(value.data() != nullptr, K_INVALID, "Can't put null pointer.");
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(bufferSize >= 0 && bufferData != nullptr, K_INVALID,
                                                 "Buffer data is invalid.");
            const uint64_t dataSize = static_cast<uint64_t>(bufferSize);
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(value.size() > 0 && value.size() <= dataSize, K_INVALID,
                                                 "Data length must be in (0, buffer_size].");
            RETURN_IF_NOT_OK(::datasystem::MemoryCopy(
                bufferData, dataSize, reinterpret_cast<const uint8_t *>(value.data()), value.size(),
                memoryCopyThreadPool_, memcpyParallelThreshold_));
        }
        return Status::OK();
    };
    static constexpr uint64_t MIN_PARALLEL_SIZE = 500 * KB;
    static constexpr uint64_t PARALLEL_SIZE = 4 * MB_TO_BYTES;
    static constexpr size_t PARALLEL_COUNT = 32;
    const bool parallel = dataSizeSum > MIN_PARALLEL_SIZE
                          && (dataSizeSum >= PARALLEL_SIZE || buffers.size() >= PARALLEL_COUNT);
    Timer timer;
    Status rc = (!parallel || parallismNum_ == 0)
                    ? memoryCopy(0, buffers.size())
                    : Parallel::ParallelFor<size_t>(0, buffers.size(), memoryCopy, 4, parallismNum_);
    const int64_t elapsedUs = timer.ElapsedMicroSecond();
    SLOW_LOG_IF_OR_VLOG(
        INFO, elapsedUs >= TimeoutDuration::SLOW_PATH_LOG_THRESHOLD_US || rc.IsError(), 1,
        FormatString("[MSet] phase=TransportMemoryCopy costUs=%lld size=%zu keys=%zu rc=%s",
                     elapsedUs, dataSizeSum, group.keys.size(), rc.ToString()));
    RETURN_IF_NOT_OK(rc);
    return ApiDeadline::Instance().CheckApiDeadline();
}

Status ObjectClientImpl::BuildMSetRouteGroups(const std::vector<std::string> &keys,
                                              const std::vector<StringView> &values,
                                              std::vector<MSetRouteGroup> &groups)
{
    auto routing = std::atomic_load(&routing_);
    RETURN_RUNTIME_ERROR_IF_NULL(routing);
    std::unordered_map<HostPort, std::vector<std::string>> groupedKeys;
    RETURN_IF_NOT_OK(routing->SelectWorkers(keys, dataPlacementPolicy_, groupedKeys,
                                            MergeWriteTargetExclusions({})));
    std::unordered_map<std::string, size_t> valueIndexes;
    valueIndexes.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        valueIndexes.emplace(keys[i], i);
    }
    groups.reserve(groupedKeys.size());
    size_t groupedKeyCount = 0;
    for (auto &entry : groupedKeys) {
        MSetRouteGroup group;
        group.worker = entry.first;
        group.keys = std::move(entry.second);
        group.values.reserve(group.keys.size());
        for (const auto &key : group.keys) {
            auto iter = valueIndexes.find(key);
            CHECK_FAIL_RETURN_STATUS(iter != valueIndexes.end(), K_RUNTIME_ERROR, "MSet route contains unknown key");
            group.values.emplace_back(values[iter->second]);
        }
        groupedKeyCount += group.keys.size();
        groups.emplace_back(std::move(group));
    }
    CHECK_FAIL_RETURN_STATUS(groupedKeyCount == keys.size(), K_RUNTIME_ERROR, "MSet route result is incomplete");
    return Status::OK();
}

Status ObjectClientImpl::ProcessTransportMSet(const MSetRouteGroup &group, const MSetParam &param,
                                              const SetRouteContext &routeContext,
                                              client::TransportMSetResult &result,
                                              SetFailureStage &failureStage, PerfPoint &point)
{
    RETURN_RUNTIME_ERROR_IF_NULL(transportLayer_);
    const auto requestContext = BuildTransportRequestContext(routeContext);
    client::TransportCreateParam createParam;
    createParam.requestContext = requestContext;
    createParam.cacheType = param.cacheType;
    createParam.consistencyType = ConsistencyType::CAUSAL;
    createParam.writeMode = param.writeMode;
    createParam.subTimeoutMs = requestTimeoutMs_;
    std::vector<uint64_t> sizes;
    uint64_t dataSizeSum = 0;
    ComputeDataSizes(group.values, sizes, dataSizeSum);
    point.RecordAndReset(PerfKey::CLIENT_MSET_MULTICREATE);
    failureStage = SetFailureStage::CREATE;
    std::vector<std::shared_ptr<ObjectBuffer>> buffers;
    RETURN_IF_NOT_OK(transportLayer_->MCreate(routeContext.worker, group.keys, sizes, createParam, buffers));
    point.RecordAndReset(PerfKey::CLIENT_MSET_MEMCOPY);
    failureStage = SetFailureStage::TRANSFER;
    Status copyRc = MemoryCopyTransportMSetBuffers(group, buffers, dataSizeSum);
    if (copyRc.IsError()) {
        for (const auto &buffer : buffers) {
            LOG_IF_ERROR(transportLayer_->Release(*buffer, requestContext),
                         "Release routed MSet allocation after MemoryCopy failure failed");
        }
        return copyRc;
    }
    client::TransportSetParam setParam;
    setParam.requestContext = requestContext;
    setParam.ttlSecond = param.ttlSecond;
    setParam.existence = param.existence;
    setParam.subTimeoutMs = requestTimeoutMs_;
    point.RecordAndReset(PerfKey::CLIENT_MSET_MULTI_PUBLISH);
    failureStage = SetFailureStage::PUBLISH;
    Status rc = transportLayer_->MSet(buffers, setParam, result);
    if (rc.GetCode() == K_URMA_NEED_CONNECT) {
        failureStage = SetFailureStage::TRANSFER;
    }
    return rc;
}

Status ObjectClientImpl::BuildMSetRetryRouteGroups(const MSetRouteGroup &group,
                                                   const std::vector<HostPort> &excludedWorkers,
                                                   std::vector<MSetRouteGroup> &groups)
{
    auto routing = std::atomic_load(&routing_);
    RETURN_RUNTIME_ERROR_IF_NULL(routing);
    std::unordered_map<HostPort, std::vector<std::string>> groupedKeys;
    RETURN_IF_NOT_OK(routing->SelectWorkers(group.keys, dataPlacementPolicy_, groupedKeys,
                                            MergeWriteTargetExclusions(excludedWorkers)));
    std::unordered_map<std::string, size_t> valueIndexes;
    valueIndexes.reserve(group.keys.size());
    for (size_t i = 0; i < group.keys.size(); ++i) {
        valueIndexes.emplace(group.keys[i], i);
    }
    groups.reserve(groupedKeys.size());
    for (auto &entry : groupedKeys) {
        MSetRouteGroup retryGroup;
        retryGroup.worker = entry.first;
        retryGroup.keys = std::move(entry.second);
        retryGroup.values.reserve(retryGroup.keys.size());
        for (const auto &key : retryGroup.keys) {
            auto value = valueIndexes.find(key);
            CHECK_FAIL_RETURN_STATUS(value != valueIndexes.end(), K_RUNTIME_ERROR,
                                     "MSet retry route contains unknown key");
            retryGroup.values.emplace_back(group.values[value->second]);
        }
        groups.emplace_back(std::move(retryGroup));
    }
    return Status::OK();
}

Status ObjectClientImpl::ExecuteTransportMSetRetryGroups(
    const std::vector<MSetRouteGroup> &groups, const MSetParam &param,
    const std::vector<HostPort> &excludedWorkers, size_t attempt,
    std::vector<std::string> &outFailedKeys, PerfPoint &point)
{
    const size_t failedBefore = outFailedKeys.size();
    size_t objectCount = 0;
    Status lastRc;
    for (const auto &retryGroup : groups) {
        objectCount += retryGroup.keys.size();
        Status rc = ExecuteTransportMSetGroupAttempt(retryGroup, param, excludedWorkers, attempt,
                                                     outFailedKeys, point);
        if (rc.IsError()) {
            lastRc = rc;
        }
    }
    if (outFailedKeys.size() - failedBefore < objectCount) {
        return Status::OK();
    }
    return lastRc.IsError() ? lastRc : Status(K_RUNTIME_ERROR, "All rerouted MSet objects failed");
}

Status ObjectClientImpl::ExecuteTransportMSetGroupAttempt(
    const MSetRouteGroup &group, const MSetParam &param, std::vector<HostPort> excludedWorkers,
    size_t attempt, std::vector<std::string> &outFailedKeys, PerfPoint &point)
{
    Status rc = ApiDeadline::Instance().CheckApiDeadline();
    if (rc.IsError()) {
        outFailedKeys.insert(outFailedKeys.end(), group.keys.begin(), group.keys.end());
        return rc;
    }
    SetRouteContext routeContext;
    rc = BuildSetRouteContext(group.worker, routeContext);
    if (rc.IsError()) {
        outFailedKeys.insert(outFailedKeys.end(), group.keys.begin(), group.keys.end());
        return rc;
    }
    client::TransportMSetResult result;
    SetFailureStage failureStage = SetFailureStage::CREATE;
    rc = ProcessTransportMSet(group, param, routeContext, result, failureStage, point);
    if (rc.IsOk()) {
        outFailedKeys.insert(outFailedKeys.end(), result.failedKeys.begin(), result.failedKeys.end());
        return rc;
    }
    const bool safeWriteTargetReplay = result.writeTargetQuarantined
                                       && (!result.publishAttempted || result.publishDefinitelyNotSent);
    if (!HandleSetRouteFailure(rc, failureStage, routeContext.worker, excludedWorkers, safeWriteTargetReplay)
        || attempt + 1 >= SET_ROUTE_MAX_ATTEMPTS) {
        const auto &failedKeys = result.failedKeys.empty() ? group.keys : result.failedKeys;
        outFailedKeys.insert(outFailedKeys.end(), failedKeys.begin(), failedKeys.end());
        return rc;
    }
    if (std::find(excludedWorkers.begin(), excludedWorkers.end(), routeContext.worker) == excludedWorkers.end()) {
        excludedWorkers.emplace_back(routeContext.worker);
    }
    std::vector<MSetRouteGroup> retryGroups;
    rc = BuildMSetRetryRouteGroups(group, excludedWorkers, retryGroups);
    if (rc.IsError()) {
        outFailedKeys.insert(outFailedKeys.end(), group.keys.begin(), group.keys.end());
        return rc;
    }
    return ExecuteTransportMSetRetryGroups(retryGroups, param, excludedWorkers, attempt + 1, outFailedKeys, point);
}

Status ObjectClientImpl::ExecuteTransportMSetGroup(const MSetRouteGroup &group, const MSetParam &param,
                                                   std::vector<std::string> &outFailedKeys, PerfPoint &point)
{
    return ExecuteTransportMSetGroupAttempt(group, param, {}, 0, outFailedKeys, point);
}

Status ObjectClientImpl::MSetThroughTransport(const std::vector<std::string> &keys,
                                              const std::vector<StringView> &values, const MSetParam &param,
                                              std::vector<std::string> &outFailedKeys, PerfPoint &point)
{
    std::vector<MSetRouteGroup> groups;
    RETURN_IF_NOT_OK(BuildMSetRouteGroups(keys, values, groups));
    const size_t failedBeforeMSet = outFailedKeys.size();
    Status lastRc;
    for (const auto &group : groups) {
        const size_t failedBeforeGroup = outFailedKeys.size();
        Status rc = ExecuteTransportMSetGroup(group, param, outFailedKeys, point);
        if (rc.IsError()) {
            lastRc = rc;
            if (outFailedKeys.size() == failedBeforeGroup) {
                outFailedKeys.insert(outFailedKeys.end(), group.keys.begin(), group.keys.end());
            }
        }
    }
    point.RecordAndReset(PerfKey::CLIENT_MSET_POST_PROCESS);
    if (outFailedKeys.size() - failedBeforeMSet < keys.size()) {
        return Status::OK();
    }
    return lastRc.IsError() ? lastRc : Status(K_RUNTIME_ERROR, "All objects set failed in worker");
}

Status ObjectClientImpl::MSet(const std::vector<std::string> &keys, const std::vector<StringView> &vals,
                              const MSetParam &param, std::vector<std::string> &outFailedKeys)
{
    PerfPoint point(PerfKey::CLIENT_MSET_INPUT_CHECK);
    AccessTransportTracker::Reset();
    std::vector<std::string> deduplicateKeys;
    std::vector<StringView> deduplicateVals;
    RETURN_IF_NOT_OK(CheckMultiSetInputParamValidationNtx(keys, vals, outFailedKeys, deduplicateKeys, deduplicateVals));
    RETURN_IF_NOT_OK(IsClientReady());
    ApiDeadlineGuard deadlineGuard(requestTimeoutMs_);
    GetRequestContext()->reqTimeoutDuration.InitUs(ApiDeadline::Instance().ApiRemainingUs());
    if (!enableLocalCache_) {
        const auto &filteredKeys = deduplicateKeys.empty() ? keys : deduplicateKeys;
        const auto &filteredValues = deduplicateVals.empty() ? vals : deduplicateVals;
        return MSetThroughTransport(filteredKeys, filteredValues, param, outFailedKeys, point);
    }
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    auto rc = boundMode_->MSetCreateCopyAndPublish(keys, vals, deduplicateKeys, deduplicateVals, param, workerApi,
                                                   outFailedKeys, point);
    return rc;
}

Status ObjectClientImpl::GenerateKey(std::string &key, const std::string &prefixKey)
{
    RETURN_IF_NOT_OK(CheckValidObjectKey(prefixKey));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(IsClientReady(), "Generate key failed.");

    if (prefixKey.empty()) {
        key = GetStringUuid();
    } else {
        key = prefixKey;
    }
    return Status::OK();
}

Status ObjectClientImpl::GetPrefix(const std::string &key, std::string &prefix)
{
    CHECK_FAIL_RETURN_STATUS(!key.empty(), K_INVALID, "The key is empty");
    RETURN_IF_NOT_OK(CheckValidObjectKey(key));
    prefix = key;
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
    ApiDeadlineGuard deadlineGuard(requestTimeoutMs_);
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    return workerApi->RemoveP2PLocation(objectKey, deviceId);
}

Status ObjectClientImpl::GetObjMetaInfo(const std::string &tenantId, const std::vector<std::string> &objectKeys,
                                        std::vector<ObjMetaInfo> &objMetas)
{
    RETURN_IF_NOT_OK(IsClientReady());
    ApiDeadlineGuard deadlineGuard(requestTimeoutMs_);
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
    auto access = std::make_shared<ObjectAccessRecorder>(
        AccessRecorder::Object(AccessRecorderKey::DS_HETERO_CLIENT_ASYNC_DEVDELETE));
    return asyncDevDeletePool_->Submit([this, traceContext, objKeys, access]() {
        PerfPoint perfPoint(PerfKey::HETERO_CLIENT_ASYNC_DEV_DELETE_IMPL);
        TraceGuard traceGuard = Trace::Instance().SetTraceContext(traceContext);
        AsyncResult result;
        std::vector<std::string> failList;
        result.status = DeleteDevObjects(objKeys, failList);
        result.failedList = std::move(failList);
        access->ObjectKeysSummaryRef(objKeys).Result(result.status).Record();
        return result;
    });
}

Status ObjectClientImpl::DeleteDevObjects(const std::vector<std::string> &objKeys, std::vector<std::string> &failList)
{
    ApiDeadlineGuard deadlineGuard(requestTimeoutMs_);
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
    auto markPublished = [this](const std::shared_ptr<Buffer> &buffer) {
        (void)memoryRefCount_.DecreaseRef(buffer->bufferInfo_->shmId);
        buffer->isReleased_ = true;
        buffer->SetVisibility(true);
    };
    if (rsp.failed_object_keys().empty()) {
        for (auto &buffer : bufferList) {
            if (buffer != nullptr && !buffer->bufferInfo_->shmId.Empty()) {
                markPublished(buffer);
            }
        }
        if (lastRc.IsError()) {
            LOG(WARNING) << "Cannot set all the objects from worker, status:" << lastRc.ToString();
        }
        return lastRc;
    }

    auto failedSet = std::set<std::string>{ rsp.failed_object_keys().begin(), rsp.failed_object_keys().end() };
    for (auto &buffer : bufferList) {
        if (buffer != nullptr && !buffer->bufferInfo_->shmId.Empty()) {
            // If the objectKey is not in the failed set, it means the worker has successfully decreased the reference
            // count. The buffer should not notify the worker again when it is being destructed.
            if (failedSet.find(buffer->bufferInfo_->objectKey) == failedSet.end()) {
                markPublished(buffer);
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
                                      const std::vector<const DeviceBlobList *> &deviceBlobRefs,
                                      std::vector<std::string> *outLocalSetKeys)
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
        .isReplica = true,
        .existence = setParam.existence,
        .ttlSecond = setParam.ttlSecond,
        .returnLocalPublishedIndexes = outLocalSetKeys != nullptr
    };
    MultiPublishRspPb rsp;
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
    RETURN_IF_NOT_OK(workerApi->MultiPublish(bufferInfoList, param, rsp, deviceBlobRefs));
    auto publishStatus = HandleShmRefCountAfterMultiPublish(bufferList, rsp);
    if (outLocalSetKeys != nullptr) {
        outLocalSetKeys->clear();
        for (auto index : rsp.local_published_indexes()) {
            CHECK_FAIL_RETURN_STATUS(
                index < bufferList.size(), K_RUNTIME_ERROR,
                FormatString("The response local published index %u exceeds the request size %zu.", index,
                             bufferList.size()));
        }
        outLocalSetKeys->reserve(rsp.local_published_indexes_size());
        for (auto index : rsp.local_published_indexes()) {
            outLocalSetKeys->emplace_back(bufferList[index]->bufferInfo_->objectKey);
        }
    }
    return publishStatus;
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

Status ObjectClientImpl::SetWorkerHealthCallback(std::function<void(const Status &)> callback,
                                                 uint32_t intervalMs)
{
    // Validate arguments first so that an invalid call leaves the existing probe thread and
    // callback untouched. The lifecycle critical section below has side effects (joins the old
    // thread, swaps the callback, creates a new thread); we must not enter it for a call that
    // will fail validation.
    if (callback != nullptr) {
        RETURN_IF_NOT_OK(IsClientReady());
        CHECK_FAIL_RETURN_STATUS(intervalMs > 0, K_INVALID,
                                 "SetWorkerHealthCallback: intervalMs must be > 0.");
    }

    // Lifecycle critical section: stop-old, swap-callback, start-new / clear are atomic with
    // respect to any concurrent Set or Stop. The probe thread never takes this mutex, and
    // ShutDown calls us before tearing down connections, so join here cannot deadlock.
    // After joining the old probe thread, workerHealthCb_ is safe to write without extra
    // locking: no probe thread is alive to read it, and the new probe is started only after.
    std::lock_guard<std::mutex> lk(workerHealthProbeLifecycleMux_);

    // Stop and join any existing probe thread first so the old callback is guaranteed not to
    // fire after we swap in the new one (or clear it).
    if (workerHealthProbeThread_ != nullptr) {
        workerHealthProbeStop_.store(true);
        workerHealthProbeExitPost_.Set();
        if (workerHealthProbeThread_->joinable()) {
            workerHealthProbeThread_->join();
        }
        workerHealthProbeThread_.reset();
    }

    // Clearing path: leave the probe stopped and the callback nulled.
    if (callback == nullptr) {
        workerHealthCb_ = nullptr;
        return Status::OK();
    }

    // Install the new callback and start a fresh probe thread.
    workerHealthCb_ = std::move(callback);
    workerHealthIntervalMs_ = intervalMs;
    workerHealthProbeStop_.store(false);
    workerHealthProbeExitPost_.Clear();
    workerHealthProbeThread_ = std::make_unique<Thread>([this] { WorkerHealthProbeLoop(); });
    workerHealthProbeThread_->set_name("HealthProbe");
    return Status::OK();
}

void ObjectClientImpl::StopWorkerHealthProbe()
{
    // Lifecycle mutex serializes concurrent stop/set/shutdown so that thread join, callback
    // swap, and thread creation never race. The probe thread itself never takes this mutex,
    // and ShutDown calls us before tearing down connections, so join here cannot deadlock.
    // After join returns, workerHealthCb_ is safe to write without extra locking.
    std::lock_guard<std::mutex> lk(workerHealthProbeLifecycleMux_);
    if (workerHealthProbeThread_ == nullptr) {
        workerHealthCb_ = nullptr;  // Already stopped; also clear any leftover callback.
        return;
    }
    workerHealthProbeStop_.store(true);
    workerHealthProbeExitPost_.Set();
    if (workerHealthProbeThread_->joinable()) {
        workerHealthProbeThread_->join();
    }
    workerHealthProbeThread_.reset();
    workerHealthCb_ = nullptr;
    // Leave workerHealthProbeStop_ true; Set clears it when starting a new probe.
}

void ObjectClientImpl::WorkerHealthProbeLoop()
{
    LOG(INFO) << "Worker health probe thread started, intervalMs=" << workerHealthIntervalMs_;
    // Two exit paths cooperate: the loop condition catches Set() that arrives between
    // iterations (no WaitFor pending), and WaitFor returning true catches Set() that
    // arrives while sleeping. Both must be checked for prompt, race-free shutdown.
    while (!workerHealthProbeStop_.load()) {
        // Probe current connected worker. Status semantics match KVClient::HealthCheck().
        // GetAvailableWorkerApi returns OK and a non-null workerApi when a worker is connected;
        // any other outcome (client not ready, no worker, raii creation failure) is surfaced
        // directly as the probe status so the caller observes the same error code as HealthCheck()
        // would return under the same conditions.
        std::shared_ptr<IClientWorkerApi> workerApi;
        std::unique_ptr<Raii> raii;
        Status probeRc = GetAvailableWorkerApi(workerApi, raii);
        if (probeRc.IsOk()) {
            // GetAvailableWorkerApi guarantees workerApi is non-null on OK.
            ServerState state = ServerState::NORMAL;
            probeRc = workerApi->HealthCheck(state);
        }
        // Invoke the callback. No lock needed: SetWorkerHealthCallback joins this thread
        // before swapping workerHealthCb_, so the field is stable while we read it here.
        // The callback contract forbids calling KVClient APIs from within.
        std::function<void(const Status &)> cb = workerHealthCb_;
        if (cb) {
            // The user callback runs on the probe thread; Thread::WrapFn is noexcept, so an
            // escaping exception would terminate the whole client process. Catch here to
            // isolate callback bugs from SDK availability.
            try {
                cb(probeRc);
            } catch (const std::exception &e) {
                LOG(ERROR) << "Worker health callback threw: " << e.what();
            } catch (...) {
                LOG(ERROR) << "Worker health callback threw unknown exception.";
            }
        }
        // Wait for the interval or until stop is signaled.
        if (workerHealthProbeExitPost_.WaitFor(workerHealthIntervalMs_)) {
            break;
        }
    }
    LOG(INFO) << "Worker health probe thread exiting.";
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

void ObjectClientImpl::ShutdownPiplnMsgQueueThread()
{
    for (size_t i = 0; i < workerApi_.size(); i++) {
        if (workerApi_[i]) {
            // close pipeline message consuming server before disconnect
            (void)workerApi_[i]->CleanUpForPipelineRH2DQueueAfterWorkerLost();
        }
    }
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

Status ObjectClientImpl::Exist(const std::vector<std::string> &keys, std::vector<bool> &exists, const bool queryL2Cache,
                               const bool isLocal)
{
    std::shared_lock<std::shared_timed_mutex> shutdownLck(shutdownMux_);
    PerfPoint perfPoint(PerfKey::CLIENT_EXIST);
    RETURN_IF_NOT_OK(IsClientReady());
    ApiDeadlineGuard deadlineGuard(requestTimeoutMs_);
    RETURN_IF_NOT_OK(CheckValidObjectKeyVector(keys));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsExistBatchSizeUnderLimit(keys.size()), K_INVALID,
                                         FormatString("The objectKeys size exceed %d.", EXIST_KEYS_MAX_SIZE_LIMIT));
    auto config = GetClientLatencyTraceConfig();
    const bool traceEnabled = ShouldCollectLatencyTrace(config);
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_EXIST_START);
    }
    Timer timer;
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_EXIST_RPC_START);
    }
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    if (enableLocalCache_) {
        RETURN_IF_NOT_OK(GetAvailableWorkerApi(workerApi, raii));
        CHECK_FAIL_RETURN_STATUS(workerApi != nullptr, K_RUNTIME_ERROR, "No available worker API for Exist");
    }
    const auto tokenPtr = std::atomic_load(&transportToken_);
    SensitiveValue token = (tokenPtr != nullptr) ? *tokenPtr : SensitiveValue();
    Status rc =
        RunExist(std::atomic_load(&routing_), transportLayer_, workerApi, keys, exists, queryL2Cache, isLocal, token);
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_EXIST_RPC_END);
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_EXIST_END);
    }
    EmitClientLatencySummary(LatencyTickKey::CLIENT_EXIST_START, LatencyTickKey::CLIENT_EXIST_END);
    const auto elapsedUs = static_cast<uint64_t>(timer.ElapsedMicroSecond());
    const double elapsedMs = static_cast<double>(elapsedUs) / US_PER_MS;
    const auto &firstKey = keys.empty() ? "" : keys[0];
    SLOW_LOG_IF_OR_VLOG(INFO, config.rpcSlowerThanUs > 0 && elapsedUs >= config.rpcSlowerThanUs, 1,
        FormatString("Finished check exist from worker, first object_key: %s, cost: %.3fms, rc: %s",
                     firstKey, elapsedMs, rc.ToString()));
    perfPoint.Record();
    return rc;
}

Status ObjectClientImpl::RunExist(std::shared_ptr<client::Routing> routing,
    std::unique_ptr<client::TransportLayer> &transportLayer, std::shared_ptr<IClientWorkerApi> &workerApi,
    const std::vector<std::string> &keys, std::vector<bool> &exists, const bool queryL2Cache, const bool isLocal,
    const SensitiveValue &token)
{
    if (routing != nullptr && transportLayer != nullptr) {
        ExistHandlerRequest request{ keys, queryL2Cache, isLocal, requestTimeoutMs_, GetClientId(),
            GetRequestContext()->tenantId.empty() ? tenantId_ : GetRequestContext()->tenantId, token };
        // Stack-allocated adapters: avoids two make_shared control-block allocations per
        // Exist call. The handler holds non-owning aliased shared_ptr to them; Run is
        // synchronous so the adapters outlive the handler.
        RoutingExistAdapter existRouting(std::move(routing));
        TransportLayerExistAdapter existTransport(transportLayer.get());
        ExistHandler flow(&existRouting, &existTransport, asyncGetRPCPool_);
        return flow.Run(request, exists);
    }
    CHECK_FAIL_RETURN_STATUS(workerApi != nullptr, K_RUNTIME_ERROR, "No available worker API for Exist");
    return workerApi->Exist(keys, exists, queryL2Cache, isLocal);
}

Status ObjectClientImpl::Expire(const std::vector<std::string> &keys, uint32_t ttlSeconds,
                                std::vector<std::string> &failedKeys)
{
    PerfPoint perfPoint(PerfKey::CLIENT_EXPIRE_OBJECT);
    ApiDeadlineGuard deadlineGuard(requestTimeoutMs_);
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
    ApiDeadlineGuard deadlineGuard(requestTimeoutMs_);
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
    }
    RETURN_IF_NOT_OK(SetClientRemoteH2DConfig(enableRemoteH2D_, devId, ipAddress_.Host()));
    devId_ = devId;
    return Status::OK();
}

std::string ObjectClientImpl::GetTransportType() const
{
    return AccessTransportTracker::ToString();
}

void ObjectClientImpl::WarmupClientWorkerConnection()
{
    bool skipWarmup = false;
    INJECT_POINT_NO_RETURN("ObjectClientImpl.ClientWorkerWarmup.skip", [&skipWarmup]() { skipWarmup = true; });
    if (skipWarmup) {
        LOG(INFO) << "[CLIENT_WORKER_WARMUP] skip by inject";
        return;
    }
    Timer timer;
    auto rc = DoWarmupClientWorkerConnection();
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("[CLIENT_WORKER_WARMUP] failed, cost_us=%.0f, status=%s",
                                     timer.ElapsedMicroSecond(), rc.ToString());
    } else {
        LOG(INFO) << FormatString("[CLIENT_WORKER_WARMUP] done, cost_us=%.0f", timer.ElapsedMicroSecond());
    }
}

Status ObjectClientImpl::WarmupOneClientWorkerConnection(const std::string &key, const std::string &value,
                                                         const SetParam &setParam, TimeoutDuration &warmupBudget,
                                                         std::vector<Optional<Buffer>> &buffers,
                                                         std::vector<std::string> &warmupKeys)
{
    int32_t remainingMs = static_cast<int32_t>(warmupBudget.CalcRealRemainingTime());
    CHECK_FAIL_RETURN_STATUS(remainingMs > 0, K_RPC_DEADLINE_EXCEEDED,
                             "Client-worker Set/Get warmup budget exhausted");
    auto rc = Set(key, StringView(value), setParam, remainingMs);
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("[CLIENT_WORKER_WARMUP] set failed, key=%s, status=%s", key, rc.ToString());
        return rc;
    }
    warmupKeys.emplace_back(key);
    INJECT_POINT_NO_RETURN("ObjectClientImpl.ClientWorkerWarmup.SetDone");
    buffers.clear();
    remainingMs = static_cast<int32_t>(warmupBudget.CalcRealRemainingTime());
    CHECK_FAIL_RETURN_STATUS(remainingMs > 0, K_RPC_DEADLINE_EXCEEDED,
                             "Client-worker Set/Get warmup budget exhausted");
    rc = Get({ key }, 0, buffers, true, false, remainingMs);
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("[CLIENT_WORKER_WARMUP] get failed, key=%s, status=%s", key, rc.ToString());
        return rc;
    }
    INJECT_POINT_NO_RETURN("ObjectClientImpl.ClientWorkerWarmup.GetDone");
    return Status::OK();
}

void ObjectClientImpl::CleanupWarmupObjects(const std::vector<std::string> &warmupKeys)
{
    if (warmupKeys.empty()) {
        return;
    }
    std::vector<std::string> failedObjectKeys;
    auto rc = Delete(warmupKeys, failedObjectKeys);
    INJECT_POINT_NO_RETURN("ObjectClientImpl.ClientWorkerWarmup.DeleteDone");
    if (rc.IsError() || !failedObjectKeys.empty()) {
        LOG(WARNING) << FormatString("[CLIENT_WORKER_WARMUP] cleanup failed, key=%s, status=%s, failed=%s",
                                     warmupKeys.front(), rc.ToString(), VectorToString(failedObjectKeys));
    }
}

Status ObjectClientImpl::DoWarmupClientWorkerConnection()
{
    try {
        constexpr uint32_t warmupTtlSecond = 5;
        // Split warmup into two phases: same-node (large value, exercises the SHM
        // fd-passing path) and meta-owner (small value, exercises the cross-node
        // RPC path). 20/80 split favors the metadata path which dominates traffic.
        constexpr int32_t warmupTimeoutMs = 500;
        constexpr size_t sameNodeCount = 20;
        constexpr size_t metaOwnerCount = 80;
        const std::string warmupKeyPrefix = "ds_internal_warmup_" + GetStringUuid();
        static const std::string sameNodeValue(256 * 1024, '0');
        static const std::string metaOwnerValue = "0";
        const auto &sameNodeWarmupValue = IsUrmaEnabled() ? sameNodeValue : metaOwnerValue;
        std::vector<std::string> warmupKeys;
        warmupKeys.reserve(sameNodeCount + metaOwnerCount);
        Raii cleanupWarmupObjects([this, &warmupKeys]() { CleanupWarmupObjects(warmupKeys); });
        const auto savedPolicy = dataPlacementPolicy_;
        Raii restorePolicy([&]() { dataPlacementPolicy_ = savedPolicy; });
        SetParam setParam;
        setParam.writeMode = WriteMode::NONE_L2_CACHE_EVICT;
        setParam.ttlSecond = warmupTtlSecond;
        TimeoutDuration warmupBudget(warmupTimeoutMs);
        warmupBudget.InitWithPositiveTime(warmupTimeoutMs);
        LOG(INFO) << FormatString("[CLIENT_WORKER_WARMUP] begin, business_timeout_ms=%d, budget_ms=%d",
                                  requestTimeoutMs_, warmupTimeoutMs);
        std::vector<Optional<Buffer>> buffers;
        // Phase 1: same-node workers (REQUIRED_SAME_NODE when localcache=false).
        dataPlacementPolicy_ = enableLocalCache_
            ? savedPolicy
            : client::DataPlacementPolicy::REQUIRED_SAME_NODE;
        for (size_t i = 0; i < sameNodeCount; ++i) {
            RETURN_IF_NOT_OK(WarmupOneClientWorkerConnection(
                warmupKeyPrefix + "_" + std::to_string(i), sameNodeWarmupValue, setParam, warmupBudget, buffers,
                warmupKeys));
        }
        // Phase 2: meta-owner (hash-ring owner, may be cross-node RPC).
        dataPlacementPolicy_ = client::DataPlacementPolicy::PREFERRED_META_OWNER;
        for (size_t i = 0; i < metaOwnerCount; ++i) {
            RETURN_IF_NOT_OK(WarmupOneClientWorkerConnection(
                warmupKeyPrefix + "_m" + std::to_string(i), metaOwnerValue, setParam, warmupBudget, buffers,
                warmupKeys));
        }
        LOG(INFO) << FormatString("[CLIENT_WORKER_WARMUP] success, prefix=%s, sameNode=%zu, metaOwner=%zu",
                                  warmupKeyPrefix, sameNodeCount, metaOwnerCount);
        return Status::OK();
    } catch (const std::exception &e) {
        LOG(WARNING) << FormatString("[CLIENT_WORKER_WARMUP] exception, error=%s", e.what());
        return Status(K_RUNTIME_ERROR, e.what());
    }
}

bool ObjectClientImpl::SubmitUrmaDataPlaneSwitch(WorkerNode node,
                                                 std::weak_ptr<client::IClientWorkerCommonApi> weakWorkerApi)
{
    return failover_->SubmitUrmaDataPlaneSwitch(node, std::move(weakWorkerApi));
}

bool ObjectClientImpl::SubmitUnavailableWorkerSwitch(const std::shared_ptr<IClientWorkerApi> &workerApi)
{
    return failover_->SubmitUnavailableWorkerSwitch(workerApi);
}

void ObjectClientImpl::DrainAsyncSwitchWorkerPool()
{
    failover_->DrainAsyncSwitchWorkerPool();
}

void ObjectClientImpl::DecreaseReferenceCnt(const ShmKey &shmId, bool isShm, uint32_t version)
{
    std::shared_lock<std::shared_timed_mutex> lck(shutdownMux_);
    boundMode_->DecreaseReferenceCnt(shmId, isShm, version);
}

Status ObjectClientImpl::Seal(const std::shared_ptr<ObjectBufferInfo> &bufferInfo,
                              const std::unordered_set<std::string> &nestedObjectKeys, bool isShm)
{
    std::shared_lock<std::shared_timed_mutex> shutdownLck(shutdownMux_);
    return boundMode_->Seal(bufferInfo, nestedObjectKeys, isShm);
}

Status ObjectClientImpl::Publish(const std::shared_ptr<ObjectBufferInfo> &bufferInfo,
                                 const std::unordered_set<std::string> &nestedObjectKeys, bool isShm)
{
    std::shared_lock<std::shared_timed_mutex> shutdownLck(shutdownMux_);
    return boundMode_->Publish(bufferInfo, nestedObjectKeys, isShm);
}

Status ObjectClientImpl::GIncreaseRef(const std::vector<std::string> &firstIncIds,
                                      std::vector<std::string> &failedObjectKeys, const std::string &remoteClientId)
{
    return boundMode_->GIncreaseRef(firstIncIds, failedObjectKeys, remoteClientId);
}

Status ObjectClientImpl::GDecreaseRef(const std::vector<std::string> &finishDecIds,
                                      std::vector<std::string> &failedObjectKeys, const std::string &remoteClientId)
{
    return boundMode_->GDecreaseRef(finishDecIds, failedObjectKeys, remoteClientId);
}

Status ObjectClientImpl::ReleaseGRefs(const std::string &remoteClientId)
{
    return boundMode_->ReleaseGRefs(remoteClientId);
}

std::shared_future<AsyncResult> ObjectClientImpl::GetWithOsTransportPipeline(
    const std::vector<std::string> &objectKeys, const std::vector<Blob> &devBlob,
    std::vector<std::shared_ptr<Buffer>> &buffers, void *h2dStream)
{
    return boundMode_->GetWithOsTransportPipeline(objectKeys, devBlob, buffers, h2dStream);
}

Status ObjectClientImpl::SendBufferViaUb(const std::shared_ptr<ObjectBufferInfo> &bufferInfo, const void *data,
                                         uint64_t length, bool traceEnabled)
{
    return boundMode_->SendBufferViaUb(bufferInfo, data, length, traceEnabled);
}

Status ObjectClientImpl::SendBufferViaUbFromPool(const std::shared_ptr<ObjectBufferInfo> &bufferInfo, const void *data,
                                                 uint64_t length, bool traceEnabled)
{
    return boundMode_->SendBufferViaUbFromPool(bufferInfo, data, length, traceEnabled);
}

Status ObjectClientImpl::InvalidateBuffer(const std::string &objectKey)
{
    return boundMode_->InvalidateBuffer(objectKey);
}

Status ObjectClientImpl::MmapShmUnit(int64_t fd, uint64_t mmapSize, ptrdiff_t offset,
                                     std::shared_ptr<client::IMmapTableEntry> &mmapEntry, uint8_t *&pointer)
{
    return boundMode_->MmapShmUnit(fd, mmapSize, offset, mmapEntry, pointer);
}

}  // namespace object_cache
}  // namespace datasystem
