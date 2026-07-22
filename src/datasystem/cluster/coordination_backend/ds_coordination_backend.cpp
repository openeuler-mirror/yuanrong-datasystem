/*
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

/**
 * Description: Coordinator-backed cluster coordination implementation.
 */
#include "datasystem/cluster/coordination_backend/ds_coordination_backend.h"

#include <algorithm>
#include <exception>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/coordination_keys.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/compatibility_manager.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uuid_generator.h"

DS_DECLARE_string(host_id_env_name);
DS_DECLARE_string(log_dir);
DS_DECLARE_uint32(node_timeout_s);

namespace datasystem::cluster {
namespace {
constexpr size_t COORDINATOR_ID_LOG_PREFIX_SIZE = 8;
}

struct DsCoordinationBackend::KeepAliveFailureState {
    int confirmMinTimes{ 3 };
    int confirmTimes{ 0 };
    bool needHandleFailure{ true };
};

DsCoordinationBackend::DsCoordinationBackend(ICoordinatorServiceProxy *proxy, std::string watcherAddr)
    : proxy_(proxy), watcherAddr_(std::move(watcherAddr))
{
}

std::unique_ptr<ICoordinationBackend> CreateDsCoordinationBackend(ICoordinatorServiceProxy *proxy,
                                                                  std::string watcherAddr)
{
    return std::make_unique<DsCoordinationBackend>(proxy, std::move(watcherAddr));
}

DsCoordinationBackend::~DsCoordinationBackend()
{
    LOG_IF_ERROR(Shutdown(), "Shut down DsCoordinationBackend failed");
}

Status DsCoordinationBackend::GetAll(const std::string &tableName,
                                     std::vector<std::pair<std::string, std::string>> &outKeyValues)
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    std::string prefix;
    RETURN_IF_NOT_OK(GetStorePrefix(tableName, prefix));
    const std::string rangeKey = prefix + "/";
    std::vector<KeyValueEntry> kvs;
    int64_t revision = 0;
    auto rc = proxy_->Range(rangeKey, StringPlusOne(rangeKey), kvs, revision);
    RefreshWatchIdentity(rc);
    RETURN_IF_NOT_OK(rc);
    outKeyValues.reserve(outKeyValues.size() + kvs.size());
    for (auto &kv : kvs) {
        outKeyValues.emplace_back(RemoveTablePrefix(kv.key, prefix), std::move(kv.value));
    }
    return Status::OK();
}

Status DsCoordinationBackend::Get(const std::string &tableName, const std::string &key, std::string &value)
{
    RangeSearchResult res;
    RETURN_IF_NOT_OK(Get(tableName, key, res));
    value = std::move(res.value);
    return Status::OK();
}

Status DsCoordinationBackend::Get(const std::string &tableName, const std::string &key, RangeSearchResult &res,
                                  int32_t timeoutMs)
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    std::vector<KeyValueEntry> kvs;
    int64_t revision = 0;
    auto rc = proxy_->Range(BuildRealKey(tableName, key), "", kvs, revision, timeoutMs);
    RefreshWatchIdentity(rc);
    RETURN_IF_NOT_OK(rc);
    if (kvs.empty()) {
        RETURN_STATUS(K_NOT_FOUND, "The key does not exist in coordinator. key:" + key);
    }
    CHECK_FAIL_RETURN_STATUS(kvs.size() == 1, K_KVSTORE_ERROR, "Coordinator key value is not unique. key:" + key);
    res.key = kvs.front().key;
    res.value = kvs.front().value;
    res.version = kvs.front().version;
    res.modRevision = kvs.front().modRevision;
    return Status::OK();
}

Status DsCoordinationBackend::CreateTable(const std::string &tableName, const std::string &tablePrefix)
{
    (void)tableName;
    (void)tablePrefix;
    return Status::OK();
}

Status DsCoordinationBackend::CreateTableWithExactPrefix(const std::string &tableName, const std::string &tablePrefix)
{
    (void)tableName;
    (void)tablePrefix;
    return Status::OK();
}

Status DsCoordinationBackend::Put(const std::string &tableName, const std::string &key, const std::string &value)
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    int64_t version = 0;
    int64_t revision = 0;
    auto rc = proxy_->Put(BuildRealKey(tableName, key), value, 0, COORDINATOR_NO_VERSION_CHECK, version, revision);
    RefreshWatchIdentity(rc);
    return rc;
}

Status DsCoordinationBackend::CAS(const std::string &tableName, const std::string &key,
                                  const ProcessFunction &processFunc, RangeSearchResult &res)
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    CHECK_FAIL_RETURN_STATUS(processFunc != nullptr, K_INVALID, "Coordinator process function resolve error.");
    const std::string realKey = BuildRealKey(tableName, key);
    int64_t version = 0;
    int64_t revision = 0;
    std::string valueFromCas;
    auto coordinatorProcessFunc = [&processFunc, &valueFromCas](const std::string &oldValue,
                                                                std::unique_ptr<std::string> &newValue,
                                                                bool &retry) -> Status {
        valueFromCas = oldValue;
        RETURN_IF_NOT_OK(processFunc(oldValue, newValue, retry));
        if (newValue != nullptr) {
            valueFromCas = *newValue;
        }
        return Status::OK();
    };
    auto rc = proxy_->CAS(realKey, coordinatorProcessFunc, version, revision);
    RefreshWatchIdentity(rc);
    RETURN_IF_NOT_OK(rc);
    res.key = realKey;
    res.value = std::move(valueFromCas);
    res.version = version;
    res.modRevision = revision;
    return Status::OK();
}

Status DsCoordinationBackend::CAS(const std::string &tableName, const std::string &key,
                                  const ProcessFunction &processFunc)
{
    RangeSearchResult res;
    return CAS(tableName, key, processFunc, res);
}

Status DsCoordinationBackend::CAS(const std::string &tableName, const std::string &key, const std::string &oldValue,
                                  const std::string &newValue)
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    std::vector<KeyValueEntry> kvs;
    int64_t revision = 0;
    std::string coordinatorId;
    const std::string realKey = BuildRealKey(tableName, key);
    auto rangeStatus = proxy_->Range(realKey, "", kvs, revision, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS, &coordinatorId);
    RefreshWatchIdentity(rangeStatus);
    RETURN_IF_NOT_OK(rangeStatus);
    if (kvs.empty()) {
        int64_t version = 0;
        auto rc = proxy_->Put(realKey, newValue, 0, COORDINATOR_KEY_NOT_EXISTS_VERSION, version, revision,
                              DEFAULT_COORDINATOR_RPC_TIMEOUT_MS, nullptr, coordinatorId);
        RefreshWatchIdentity(rc);
        return rc;
    }
    CHECK_FAIL_RETURN_STATUS(kvs.front().value == oldValue, K_TRY_AGAIN, "Coordinator compare value failed");
    int64_t version = 0;
    auto rc = proxy_->Put(realKey, newValue, 0, kvs.front().version, version, revision,
                          DEFAULT_COORDINATOR_RPC_TIMEOUT_MS, nullptr, coordinatorId);
    RefreshWatchIdentity(rc);
    return rc;
}

Status DsCoordinationBackend::Delete(const std::string &tableName, const std::string &key)
{
    return Delete(tableName, key, DEFAULT_COORDINATION_DELETE_TIMEOUT_MS);
}

Status DsCoordinationBackend::Delete(const std::string &tableName, const std::string &key, int timeoutMs)
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    int64_t deleted = 0;
    int64_t revision = 0;
    auto rc = proxy_->DeleteRange(BuildRealKey(tableName, key), "", deleted, revision, timeoutMs);
    RefreshWatchIdentity(rc);
    return rc;
}

Status DsCoordinationBackend::WatchEvents(const std::vector<WatchKey> &watchKeys)
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    std::lock_guard<std::mutex> lock(rewatchMutex_);
    return RegisterWatchPlan(watchKeys);
}

Status DsCoordinationBackend::RegisterWatchPlan(const std::vector<WatchKey> &watchKeys)
{
    {
        std::lock_guard<std::mutex> lock(watchMutex_);
        CHECK_FAIL_RETURN_STATUS(!watchStopping_ && !watchKeys.empty(), K_NOT_READY,
                                 "Coordinator watch backend is stopping or has an empty plan");
        watchRegistrationInProgress_ = true;
    }
    Raii clearRegistration([this] {
        std::lock_guard<std::mutex> lock(watchMutex_);
        watchRegistrationInProgress_ = false;
    });
    std::vector<WatchRegistration> registrations;
    std::vector<int64_t> registeredIds;
    std::vector<CoordinationEvent> initialEvents;
    std::string batchCoordinatorId;
    if (pendingWatchRegistrationId_.empty()) {
        pendingWatchRegistrationId_ = GetBytesUuid();
    }
    RETURN_IF_NOT_OK(PrepareWatchPlan(watchKeys, registrations, registeredIds, initialEvents, batchCoordinatorId));
    std::string observedCoordinatorId;
    proxy_->GetObservedCoordinatorId(observedCoordinatorId);
    if (batchCoordinatorId.empty() || batchCoordinatorId != observedCoordinatorId) {
        LOG_IF_ERROR(proxy_->CancelWatch(watcherAddr_, registeredIds, batchCoordinatorId),
                     "Rollback stale Coordinator watch batch");
        RETURN_STATUS(K_TRY_AGAIN, "Coordinator watch batch became stale before commit");
    }
    CommitWatchPlan(watchKeys, std::move(registrations), batchCoordinatorId);
    pendingWatchRegistrationId_.clear();
    for (auto &event : initialEvents) {
        DispatchWatchEvent(std::move(event));
    }
    DispatchWatchEvent({ CoordinationEventType::RESET, "", "", 0, 0 });
    return Status::OK();
}

Status DsCoordinationBackend::PrepareWatchPlan(const std::vector<WatchKey> &watchKeys,
                                               std::vector<WatchRegistration> &registrations,
                                               std::vector<int64_t> &registeredIds,
                                               std::vector<CoordinationEvent> &initialEvents,
                                               std::string &coordinatorId)
{
    for (const auto &watchKey : watchKeys) {
        const std::string realKey = BuildRealKey(watchKey.tableName, watchKey.key);
        const bool isPrefix = watchKey.key.empty();
        const std::string rangeEnd = isPrefix ? StringPlusOne(realKey) : "";
        std::vector<KeyValueEntry> initialKvs;
        int64_t watchId = 0;
        std::string responseCoordinatorId;
        auto rc = proxy_->WatchRange(realKey, rangeEnd, watcherAddr_, pendingWatchRegistrationId_ + realKey, watchId,
                                     initialKvs, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS, &responseCoordinatorId);
        if (rc.IsOk() && !coordinatorId.empty() && coordinatorId != responseCoordinatorId) {
            LOG_IF_ERROR(proxy_->CancelWatch(watcherAddr_, { watchId }, responseCoordinatorId),
                         "Cancel current-generation watch");
            LOG_IF_ERROR(proxy_->CancelWatch(watcherAddr_, registeredIds, coordinatorId),
                         "Cancel previous-generation partial watches");
            RETURN_STATUS(K_TRY_AGAIN, "CoordinatorId changed during watch registration");
        }
        if (rc.IsError()) {
            if (!registeredIds.empty()) {
                LOG_IF_ERROR(proxy_->CancelWatch(watcherAddr_, registeredIds, coordinatorId),
                             "Rollback partial Coordinator watches");
            }
            return rc;
        }
        registrations.push_back({ watchId, { realKey, isPrefix } });
        registeredIds.emplace_back(watchId);
        coordinatorId = responseCoordinatorId;
        for (auto &kv : initialKvs) {
            initialEvents.push_back(
                { CoordinationEventType::PUT, std::move(kv.key), std::move(kv.value), kv.version, kv.modRevision });
        }
    }
    return Status::OK();
}

void DsCoordinationBackend::CommitWatchPlan(const std::vector<WatchKey> &watchKeys,
                                            std::vector<WatchRegistration> registrations,
                                            const std::string &coordinatorId)
{
    std::vector<int64_t> previousWatchIds;
    std::string previousCoordinatorId;
    {
        std::lock_guard<std::mutex> lock(watchMutex_);
        previousCoordinatorId = registeredCoordinatorId_;
        for (const auto &registration : registrations_) {
            previousWatchIds.emplace_back(registration.watchId);
        }
        watchPlan_ = watchKeys;
        registrations_ = std::move(registrations);
        registeredCoordinatorId_ = coordinatorId;
        rewatchRequired_ = false;
        watchRegistrationInProgress_ = false;
    }
    LOG(INFO) << "CLUSTER_WATCH_REGISTERED watcher=" << watcherAddr_ << ", scope_count=" << watchKeys.size()
              << ", coordinator_id=" << coordinatorId.substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE);
    if (previousCoordinatorId == coordinatorId && !previousWatchIds.empty()) {
        LOG_IF_ERROR(proxy_->CancelWatch(watcherAddr_, previousWatchIds, previousCoordinatorId),
                     "Cancel replaced Coordinator watches");
    }
}

Status DsCoordinationBackend::RewatchIfNeeded()
{
    std::lock_guard<std::mutex> rewatchLock(rewatchMutex_);
    std::vector<WatchKey> plan;
    {
        std::lock_guard<std::mutex> watchLock(watchMutex_);
        if (!rewatchRequired_ || watchStopping_) {
            return Status::OK();
        }
        plan = watchPlan_;
    }
    return RegisterWatchPlan(plan);
}

void DsCoordinationBackend::RefreshWatchIdentity(const Status &status)
{
    std::string coordinatorId;
    const bool probe = status.GetCode() == K_NOT_READY || IsRpcTimeout(status);
    std::unique_lock<std::mutex> probeLock(rewatchMutex_, std::defer_lock);
    if (status.IsError() && !probe && status.GetCode() != K_TRY_AGAIN) {
        return;
    }
    if (probe) {
        probeLock.lock();
        const auto now = std::chrono::steady_clock::now();
        {
            std::lock_guard<std::mutex> lock(watchMutex_);
            if (watchStopping_ || watchPlan_.empty() || now < nextIdentityProbeAt_) {
                return;
            }
            nextIdentityProbeAt_ = now + identityProbeBackoff_;
            identityProbeBackoff_ =
                std::min(identityProbeBackoff_ * IDENTITY_PROBE_BACKOFF_MULTIPLIER, MAX_IDENTITY_PROBE_BACKOFF);
        }
        if (proxy_->GetCoordinatorId(coordinatorId).IsError()) {
            return;
        }
    } else {
        proxy_->GetObservedCoordinatorId(coordinatorId);
    }
    bool identityChanged = false;
    bool rewatch = false;
    {
        std::lock_guard<std::mutex> lock(watchMutex_);
        if (probe) {
            identityProbeBackoff_ = INITIAL_IDENTITY_PROBE_BACKOFF;
            nextIdentityProbeAt_ = {};
        }
        if (!coordinatorId.empty() && !watchPlan_.empty() && registeredCoordinatorId_ != coordinatorId) {
            rewatchRequired_ = true;
            identityChanged = true;
        }
        rewatch = rewatchRequired_ && (status.IsOk() || identityChanged);
    }
    if (rewatch) {
        if (probe) {
            std::vector<WatchKey> plan;
            {
                std::lock_guard<std::mutex> lock(watchMutex_);
                plan = watchPlan_;
            }
            LOG_IF_ERROR(RegisterWatchPlan(plan), "Re-register Coordinator watches after identity probe");
        } else {
            LOG_IF_ERROR(RewatchIfNeeded(), "Re-register Coordinator watches after identity observation");
        }
    }
}

Status DsCoordinationBackend::InitKeepAlive(const std::string &tableName, const std::string &key, bool isRestart,
                                            bool isStoreAvailableWhenStart)
{
    std::string hostId;
    if (!FLAGS_host_id_env_name.empty()) {
        auto envHostId = GetStringFromEnv(FLAGS_host_id_env_name.c_str(), "");
        auto envFilePath = GetWorkerEnvFilePath(FLAGS_log_dir);
        hostId = GetStringFromEnvOrFile(FLAGS_host_id_env_name.c_str(), envFilePath, FLAGS_host_id_env_name, "");
        if (hostId.empty()) {
            LOG(WARNING) << FormatString("host_id env [%s] is empty when worker registers to etcd.",
                                         FLAGS_host_id_env_name);
        } else if (envHostId.empty()) {
            LOG(INFO) << "Host id is " << hostId << " from persisted worker env file " << envFilePath;
        } else {
            LOG(INFO) << "Host id is " << hostId << " from env " << FLAGS_host_id_env_name;
        }
    } else {
        // host_id_env_name is unset: the worker registers an empty host_id, so clients cannot partition
        // same-node workers and sdk_data_placement_policy=PREFERRED_SAME_NODE silently degrades to the
        // hash ring (cross-node routing for every key, including large payloads that may time out).
        LOG(WARNING) << "host_id_env_name is not set; worker will register an empty host_id and same-node "
                        "worker affinity (sdk_data_placement_policy=PREFERRED_SAME_NODE) will be disabled. "
                        "Set --host_id_env_name=<ENV_VAR> and export <ENV_VAR>=<unique-per-host value> on each "
                        "worker host to enable same-node routing.";
    }

    keepAliveTableName_ = tableName;
    keepAliveKey_ = key;
    firstKeepAliveSent_.store(false, std::memory_order_release);
    keepAliveTtlMs_ = static_cast<int64_t>(FLAGS_node_timeout_s) * MS_PER_SECOND;
    keepAliveValue_.timestamp = 0;
    keepAliveValue_.hostId = hostId;
    keepAliveValue_.compatibilityVersion = CompatibilityManager::Instance().GetCurrentCompatibilityVersion().ToString();
    if (!isStoreAvailableWhenStart) {
        keepAliveValue_.lifecycleState = MemberLifecycleState::DOWNGRADE_RESTARTING;
    } else if (isRestart) {
        keepAliveValue_.lifecycleState = MemberLifecycleState::RESTARTING;
    } else {
        keepAliveValue_.lifecycleState = MemberLifecycleState::STARTING;
    }
    // Publishing the lease can race the previous lease's TTL delete, which also removes this address's watch channels.
    RETURN_IF_NOT_OK(AutoCreateKeepAliveKey(true));
    LaunchKeepAliveThread();
    return Status::OK();
}

Status DsCoordinationBackend::AutoCreateKeepAliveKey(bool recreated)
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    CHECK_FAIL_RETURN_STATUS(!keepAliveTableName_.empty(), K_INVALID, "Coordinator keepalive table is empty");
    CHECK_FAIL_RETURN_STATUS(!keepAliveKey_.empty(), K_INVALID, "Coordinator keepalive key is empty");
    std::lock_guard<std::mutex> mutationLock(membershipMutationMutex_);

    MembershipValue value;
    int64_t timeStamp = std::chrono::system_clock::now().time_since_epoch().count();
    {
        std::lock_guard<std::mutex> lock(keepAliveMutex_);
        CHECK_FAIL_RETURN_STATUS(keepAliveValue_.lifecycleState != MemberLifecycleState::UNKNOWN, K_INVALID,
                                 "Node state should not be empty.");
        keepAliveValue_.timestamp = timeStamp;
        value = keepAliveValue_;
    }
    std::string valueStr;
    RETURN_IF_NOT_OK(MembershipValueCodec::Encode(value, valueStr));
    int64_t version = 0;
    int64_t revision = 0;
    std::string coordinatorId;
    auto rc = proxy_->Put(BuildRealKey(keepAliveTableName_, keepAliveKey_), valueStr, keepAliveTtlMs_,
                          COORDINATOR_NO_VERSION_CHECK, version, revision, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS,
                          &coordinatorId);
    LOG(INFO) << "AutoCreateKeepAliveKey: Put keepalive key " << keepAliveKey_ << " result: " << rc.ToString();
    RETURN_IF_NOT_OK(rc);
    firstKeepAliveSent_.store(true, std::memory_order_release);
    HandleMembershipSuccess(coordinatorId, recreated);
    {
        std::lock_guard<std::mutex> lock(keepAliveMutex_);
        if (keepAliveValue_.lifecycleState == MemberLifecycleState::STARTING
            || keepAliveValue_.lifecycleState == MemberLifecycleState::RESTARTING) {
            keepAliveValue_.lifecycleState = MemberLifecycleState::RECOVERING;
        }
    }
    return Status::OK();
}

Status DsCoordinationBackend::RenewKeepAliveOnce()
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    INJECT_POINT("CoordinationBackend.KeepAlive.returnError");
    int64_t ttlMs = keepAliveTtlMs_;
    int64_t remainingTtlMs = 0;
    std::string coordinatorId;
    auto rc = proxy_->KeepAlive(BuildRealKey(keepAliveTableName_, keepAliveKey_), ttlMs, remainingTtlMs,
                                DEFAULT_COORDINATOR_RPC_TIMEOUT_MS, &coordinatorId);
    if (rc.IsOk()) {
        HandleMembershipSuccess(coordinatorId);
    }
    return rc;
}

void DsCoordinationBackend::LaunchKeepAliveThread()
{
    ShutdownKeepAliveThread();
    keepAliveExit_ = false;
    keepAliveThread_ = Thread(&DsCoordinationBackend::RunKeepAliveLoop, this);
    keepAliveThread_.set_name("cluster-coord");
}

void DsCoordinationBackend::RunKeepAliveLoop()
{
    int64_t intervalMs = keepAliveTtlMs_ / 3;
    constexpr int64_t maxIntervalMs = 5'000;
    constexpr int64_t minIntervalMs = 100;
    intervalMs = std::max(minIntervalMs, std::min(intervalMs, maxIntervalMs));
    KeepAliveFailureState state;
    INJECT_POINT("CoordinationBackend.KeepAlive.intervalMs", [&intervalMs](int timeMs) { intervalMs = timeMs; });
    INJECT_POINT("CoordinationBackend.KeepAlive.confirmTimes", [&state](int times) { state.confirmMinTimes = times; });
    const std::string realKey = BuildRealKey(keepAliveTableName_, keepAliveKey_);
    while (!keepAliveExit_) {
        auto rc = RenewKeepAliveOnce();
        VLOG(1) << "Member " << watcherAddr_ << " keepalive result: " << rc.ToString();
        if (rc.IsOk()) {
            HandleKeepAliveSuccess(state);
        } else {
            HandleKeepAliveFailure(rc, realKey, state);
        }
        std::unique_lock<std::mutex> lock(keepAliveMutex_);
        keepAliveCv_.wait_for(lock, std::chrono::milliseconds(intervalMs), [this]() { return keepAliveExit_.load(); });
    }
}

void DsCoordinationBackend::HandleKeepAliveSuccess(KeepAliveFailureState &state)
{
    LocalRecoveryHandler recoveryHandler;
    if (!state.needHandleFailure) {
        std::lock_guard<std::mutex> lock(eventHandlerMutex_);
        recoveryHandler = localRecoveryHandler_;
    }
    keepAliveTimeout_ = false;
    state.confirmTimes = 0;
    state.needHandleFailure = true;
    if (recoveryHandler != nullptr) {
        recoveryHandler();
    }
}

void DsCoordinationBackend::HandleMembershipSuccess(const std::string &coordinatorId, bool recreated)
{
    MembershipReadyHandler handler;
    bool identityChanged = false;
    {
        std::lock_guard<std::mutex> lock(eventHandlerMutex_);
        identityChanged = !coordinatorId.empty() && lastMembershipCoordinatorId_ != coordinatorId;
        lastMembershipCoordinatorId_ = coordinatorId;
        handler = membershipReadyHandler_;
    }
    bool invalidated = false;
    {
        std::lock_guard<std::mutex> lock(watchMutex_);
        if (!watchPlan_.empty() && !rewatchRequired_ && (recreated || registeredCoordinatorId_ != coordinatorId)) {
            rewatchRequired_ = true;
            invalidated = true;
        }
    }
    if (invalidated) {
        DispatchWatchEvent({ CoordinationEventType::RESET, "", "", 0, 0 });
    }
    if (identityChanged || recreated) {
        LOG(INFO) << "CLUSTER_COORDINATOR_ID role=worker watcher=" << watcherAddr_
                  << ", id=" << coordinatorId.substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE)
                  << ", membership_recreated=" << recreated << ", watches_invalidated=" << invalidated;
    }
    if (handler != nullptr) {
        handler(coordinatorId, invalidated);
    }
}

bool DsCoordinationBackend::CheckStoreAvailableAfterKeepAliveFailure(KeepAliveFailureState &state)
{
    std::function<bool()> availabilityCheck;
    {
        std::lock_guard<std::mutex> lock(eventHandlerMutex_);
        availabilityCheck = checkStoreStateWhenNetworkFailedHandler_;
    }
    if (!state.needHandleFailure || availabilityCheck == nullptr) {
        return false;
    }
    const bool available = availabilityCheck();
    if (!available) {
        state.confirmTimes = 0;
    }
    return available;
}

void DsCoordinationBackend::HandleKeepAliveFailure(const Status &status, const std::string &realKey,
                                                   KeepAliveFailureState &state)
{
    keepAliveTimeout_ = true;
    const bool storeAvailable = CheckStoreAvailableAfterKeepAliveFailure(state);
    if (storeAvailable && ++state.confirmTimes >= state.confirmMinTimes) {
        HandleKeepAliveFailed(realKey);
        LocalIsolationHandler isolationHandler;
        {
            std::lock_guard<std::mutex> lock(eventHandlerMutex_);
            isolationHandler = localIsolationHandler_;
        }
        if (isolationHandler != nullptr) {
            isolationHandler(status);
        }
        state.needHandleFailure = false;
        LOG(WARNING) << "Confirmed local Coordinator network isolation; keep the process alive and report the "
                        "membership deletion event.";
    } else if (status.GetCode() == K_NOT_FOUND) {
        (void)AutoCreateKeepAliveKey(true);
    }
}

void DsCoordinationBackend::HandleKeepAliveFailed(const std::string &realKey)
{
    CoordinationEvent event;
    event.type = CoordinationEventType::DELETE;
    event.key = realKey;
    event.value = "";
    DispatchWatchEvent(std::move(event));
}

void DsCoordinationBackend::CancelWatches()
{
    std::vector<int64_t> watchIds;
    std::string watchCoordinatorId;
    {
        std::lock_guard<std::mutex> lock(watchMutex_);
        for (const auto &registration : registrations_) {
            watchIds.emplace_back(registration.watchId);
        }
        registrations_.clear();
        watchPlan_.clear();
        watchCoordinatorId = registeredCoordinatorId_;
        registeredCoordinatorId_.clear();
        rewatchRequired_ = false;
    }
    if (proxy_ == nullptr || watchIds.empty()) {
        return;
    }
    LOG_IF_ERROR(proxy_->CancelWatch(watcherAddr_, watchIds, watchCoordinatorId), "Cancel coordinator watches failed");
}

void DsCoordinationBackend::ShutdownKeepAliveThread()
{
    keepAliveExit_ = true;
    keepAliveCv_.notify_all();
    if (keepAliveThread_.joinable()) {
        keepAliveThread_.join();
    }
}

Status DsCoordinationBackend::ShutdownEventSources()
{
    RETURN_IF_NOT_OK(ShutdownWatchEventSources());
    ShutdownKeepAliveThread();
    return Status::OK();
}

Status DsCoordinationBackend::ShutdownWatchEventSources()
{
    {
        std::lock_guard<std::mutex> lock(eventHandlerMutex_);
        eventHandler_ = {};
        membershipReadyHandler_ = {};
    }
    {
        std::lock_guard<std::mutex> rewatchLock(rewatchMutex_);
        std::lock_guard<std::mutex> lock(watchMutex_);
        watchStopping_ = true;
    }
    CancelWatches();
    std::unique_lock<std::mutex> lock(eventHandlerMutex_);
    eventHandlerCv_.wait(lock, [this] { return activeEventHandlers_ == 0; });
    return Status::OK();
}

Status DsCoordinationBackend::Shutdown()
{
    return ShutdownEventSources();
}

Status DsCoordinationBackend::UpdateNodeState(MemberLifecycleState state)
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    CHECK_FAIL_RETURN_STATUS(!IsKeepAliveTimeout(), K_NOT_READY,
                             "The key written to the cluster table must be bound to a lease");
    std::lock_guard<std::mutex> mutationLock(membershipMutationMutex_);
    MembershipValue value;
    {
        std::lock_guard<std::mutex> lock(keepAliveMutex_);
        value = keepAliveValue_;
        value.lifecycleState = state;
    }
    std::string valueStr;
    RETURN_IF_NOT_OK(MembershipValueCodec::Encode(value, valueStr));
    int64_t version = 0;
    int64_t revision = 0;
    std::string coordinatorId;
    RETURN_IF_NOT_OK(proxy_->Put(BuildRealKey(keepAliveTableName_, keepAliveKey_), valueStr, keepAliveTtlMs_,
                                 COORDINATOR_NO_VERSION_CHECK, version, revision, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS,
                                 &coordinatorId));
    {
        std::lock_guard<std::mutex> lock(keepAliveMutex_);
        keepAliveValue_ = value;
    }
    HandleMembershipSuccess(coordinatorId);
    return Status::OK();
}

Status DsCoordinationBackend::GetStorePrefix(const std::string &tableName, std::string &prefix)
{
    CHECK_FAIL_RETURN_STATUS(!tableName.empty(), K_INVALID, "Coordinator table name is empty");
    if (tableName == COORDINATION_CLUSTER_TABLE) {
        prefix = "/" + std::string(COORDINATION_CLUSTER_TABLE);
        return Status::OK();
    }
    prefix = tableName;
    return Status::OK();
}

Status DsCoordinationBackend::InformReconciliationDone(const HostPort &workerAddr)
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    std::lock_guard<std::mutex> mutationLock(membershipMutationMutex_);
    const std::string realKey = BuildRealKey(keepAliveTableName_, workerAddr.ToString());
    std::vector<KeyValueEntry> entries;
    int64_t revision = 0;
    std::string rangeCoordinatorId;
    auto rangeStatus =
        proxy_->Range(realKey, "", entries, revision, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS, &rangeCoordinatorId);
    RefreshWatchIdentity(rangeStatus);
    RETURN_IF_NOT_OK(rangeStatus);
    CHECK_FAIL_RETURN_STATUS(!entries.empty(), K_NOT_FOUND, "membership does not exist during reconciliation");
    MembershipValue value;
    RETURN_IF_NOT_OK(MembershipValueCodec::Decode(entries.front().value, value));
    if (value.lifecycleState == MemberLifecycleState::RESTARTING
        || value.lifecycleState == MemberLifecycleState::RECOVERING) {
        value.lifecycleState = MemberLifecycleState::READY;
        std::string readyValue;
        RETURN_IF_NOT_OK(MembershipValueCodec::Encode(value, readyValue));
        int64_t version = 0;
        int64_t putRevision = 0;
        std::string coordinatorId;
        auto putStatus =
            proxy_->Put(realKey, readyValue, keepAliveTtlMs_, entries.front().version, version, putRevision,
                        DEFAULT_COORDINATOR_RPC_TIMEOUT_MS, &coordinatorId, rangeCoordinatorId);
        RefreshWatchIdentity(putStatus);
        RETURN_IF_NOT_OK(putStatus);
        HandleMembershipSuccess(coordinatorId);
    }
    return Status::OK();
}

bool DsCoordinationBackend::IsKeepAliveTimeout()
{
    return keepAliveTimeout_;
}

bool DsCoordinationBackend::IsFirstKeepAliveSent()
{
    return firstKeepAliveSent_.load(std::memory_order_acquire);
}

void DsCoordinationBackend::SetEventHandler(EventHandler &&eventHandler)
{
    std::lock_guard<std::mutex> lock(eventHandlerMutex_);
    eventHandler_ = std::move(eventHandler);
}

void DsCoordinationBackend::SetLocalIsolationHandler(LocalIsolationHandler handler)
{
    std::lock_guard<std::mutex> lock(eventHandlerMutex_);
    localIsolationHandler_ = std::move(handler);
}

void DsCoordinationBackend::SetLocalRecoveryHandler(LocalRecoveryHandler handler)
{
    std::lock_guard<std::mutex> lock(eventHandlerMutex_);
    localRecoveryHandler_ = std::move(handler);
}

void DsCoordinationBackend::SetCheckStoreStateWhenNetworkFailedHandler(std::function<bool()> handler)
{
    std::lock_guard<std::mutex> lock(eventHandlerMutex_);
    checkStoreStateWhenNetworkFailedHandler_ = std::move(handler);
}

const std::string &DsCoordinationBackend::GetWatcherAddr() const
{
    return watcherAddr_;
}

void DsCoordinationBackend::SetMembershipReadyHandler(const MembershipReadyHandler &handler)
{
    std::string coordinatorId;
    MembershipReadyHandler callback;
    {
        std::lock_guard<std::mutex> lock(eventHandlerMutex_);
        membershipReadyHandler_ = handler;
        callback = membershipReadyHandler_;
        coordinatorId = lastMembershipCoordinatorId_;
    }
    if (callback != nullptr && !coordinatorId.empty()) {
        callback(coordinatorId, false);
    }
}

bool DsCoordinationBackend::OwnsWatchIdentity(const std::string &coordinatorId, int64_t watchId) const
{
    std::lock_guard<std::mutex> lock(watchMutex_);
    return !watchStopping_ && registeredCoordinatorId_ == coordinatorId
           && std::any_of(registrations_.begin(), registrations_.end(),
                          [watchId](const auto &entry) { return entry.watchId == watchId; });
}

bool DsCoordinationBackend::IsWatchRegistrationInProgress() const
{
    std::lock_guard<std::mutex> lock(watchMutex_);
    return !watchStopping_ && watchRegistrationInProgress_;
}

void DsCoordinationBackend::InvalidateWatches()
{
    bool invalidated = false;
    {
        std::lock_guard<std::mutex> lock(watchMutex_);
        invalidated = !watchStopping_ && !watchPlan_.empty() && !rewatchRequired_;
        if (invalidated) {
            rewatchRequired_ = true;
        }
    }
    if (invalidated) {
        DispatchWatchEvent({ CoordinationEventType::RESET, "", "", 0, 0 });
    }
}

void DsCoordinationBackend::HandleWatchEvent(const std::string &coordinatorId, int64_t watchId,
                                             CoordinationEvent &&event)
{
    if (!OwnsWatchIdentity(coordinatorId, watchId)) {
        return;
    }
    if (event.type == CoordinationEventType::RESET) {
        bool invalidated = false;
        {
            std::lock_guard<std::mutex> lock(watchMutex_);
            invalidated = !rewatchRequired_;
            rewatchRequired_ = true;
        }
        if (invalidated) {
            DispatchWatchEvent(std::move(event));
        }
        return;
    }
    if (AcceptsWatchEvent(watchId, event.key)) {
        DispatchWatchEvent(std::move(event));
    }
}

void DsCoordinationBackend::DispatchWatchEvent(CoordinationEvent &&event)
{
    EventHandler handler;
    {
        std::lock_guard<std::mutex> lock(eventHandlerMutex_);
        handler = eventHandler_;
        if (handler == nullptr) {
            return;
        }
        ++activeEventHandlers_;
    }
    // User event handlers must not unwind through the Coordinator RPC callback boundary.
    try {
        handler(std::move(event));
    } catch (const std::exception &error) {
        LOG(ERROR) << "Coordinator watch event handler threw: " << error.what();
    } catch (...) {
        LOG(ERROR) << "Coordinator watch event handler threw an unknown exception";
    }
    {
        std::lock_guard<std::mutex> lock(eventHandlerMutex_);
        --activeEventHandlers_;
    }
    eventHandlerCv_.notify_all();
}

bool DsCoordinationBackend::AcceptsWatchEvent(int64_t watchId, const std::string &key) const
{
    std::lock_guard<std::mutex> lock(watchMutex_);
    return std::any_of(registrations_.begin(), registrations_.end(), [&](const WatchRegistration &registration) {
        if (registration.watchId != watchId) {
            return false;
        }
        if (registration.scope.isPrefix) {
            return key.rfind(registration.scope.key, 0) == 0;
        }
        return key == registration.scope.key;
    });
}

std::string DsCoordinationBackend::RemoveTablePrefix(const std::string &key, const std::string &prefix)
{
    const std::string prefixWithSlash = prefix + "/";
    if (key.rfind(prefixWithSlash, 0) == 0) {
        return key.substr(prefixWithSlash.size());
    }
    return key;
}

std::string DsCoordinationBackend::BuildRealKey(const std::string &tableName, const std::string &key)
{
    std::string prefix;
    Status status = GetStorePrefix(tableName, prefix);
    if (status.IsError()) {
        return key;
    }
    return prefix + "/" + key;
}
}  // namespace datasystem::cluster
