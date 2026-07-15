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
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"

DS_DECLARE_string(host_id_env_name);
DS_DECLARE_string(log_dir);
DS_DECLARE_uint32(node_timeout_s);

namespace datasystem::cluster {

struct DsCoordinationBackend::KeepAliveFailureState {
    int confirmMinTimes{ 3 };
    int confirmTimes{ 0 };
    bool needHandleFailure{ true };
};

DsCoordinationBackend::DsCoordinationBackend(ICoordinatorServiceProxy *proxy, std::string watcherAddr)
    : proxy_(proxy), watcherAddr_(std::move(watcherAddr))
{
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
    RETURN_IF_NOT_OK(proxy_->Range(rangeKey, StringPlusOne(rangeKey), kvs, revision));
    outKeyValues.reserve(outKeyValues.size() + kvs.size());
    for (const auto &kv : kvs) {
        outKeyValues.emplace_back(RemoveTablePrefix(kv.key, prefix), kv.value);
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
    RETURN_IF_NOT_OK(proxy_->Range(BuildRealKey(tableName, key), "", kvs, revision, timeoutMs));
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
    RETURN_IF_NOT_OK(proxy_->CAS(realKey, coordinatorProcessFunc, version, revision));
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
    const std::string realKey = BuildRealKey(tableName, key);
    RETURN_IF_NOT_OK(proxy_->Range(realKey, "", kvs, revision));
    if (kvs.empty()) {
        int64_t version = 0;
        return proxy_->Put(realKey, newValue, 0, COORDINATOR_KEY_NOT_EXISTS_VERSION, version, revision);
    }
    CHECK_FAIL_RETURN_STATUS(kvs.front().value == oldValue, K_TRY_AGAIN, "Coordinator compare value failed");
    int64_t version = 0;
    return proxy_->Put(realKey, newValue, 0, kvs.front().version, version, revision);
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
    return proxy_->DeleteRange(BuildRealKey(tableName, key), "", deleted, revision, timeoutMs);
}

Status DsCoordinationBackend::WatchEvents(const std::vector<WatchKey> &watchKeys)
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    std::vector<int64_t> registeredWatchIds;
    std::vector<CoordinationEvent> initialEvents;
    std::vector<WatchedKey> watchedKeys;
    for (const auto &watchKey : watchKeys) {
        const std::string realKey = BuildRealKey(watchKey.tableName, watchKey.key);
        const bool isPrefix = watchKey.key.empty();
        const std::string rangeEnd = isPrefix ? StringPlusOne(realKey) : "";
        std::vector<KeyValueEntry> initialKvs;
        int64_t watchId = 0;
        auto rc = proxy_->WatchRange(realKey, rangeEnd, watcherAddr_, watchId, initialKvs);
        if (rc.IsError()) {
            if (!registeredWatchIds.empty()) {
                LOG_IF_ERROR(proxy_->CancelWatch(watcherAddr_, registeredWatchIds),
                             "Rollback registered coordinator watches failed");
            }
            return rc;
        }
        registeredWatchIds.emplace_back(watchId);
        for (const auto &kv : initialKvs) {
            CoordinationEvent event;
            event.type = CoordinationEventType::PUT;
            event.key = kv.key;
            event.value = kv.value;
            event.version = kv.version;
            event.revision = kv.modRevision;
            initialEvents.emplace_back(std::move(event));
        }
        watchedKeys.emplace_back(WatchedKey{ realKey, isPrefix });
    }
    {
        std::lock_guard<std::mutex> lock(watchMutex_);
        watchIds_.insert(watchIds_.end(), registeredWatchIds.begin(), registeredWatchIds.end());
        watchedKeys_.insert(watchedKeys_.end(), watchedKeys.begin(), watchedKeys.end());
    }
    for (auto &event : initialEvents) {
        LOG(INFO) << "WatchEvents: fake event " << event.ToString();
        HandleWatchEvent(std::move(event));
    }
    return Status::OK();
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
    }

    keepAliveTableName_ = tableName;
    keepAliveKey_ = key;
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
    RETURN_IF_NOT_OK(AutoCreateKeepAliveKey());
    LaunchKeepAliveThread();
    return Status::OK();
}

Status DsCoordinationBackend::AutoCreateKeepAliveKey()
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    CHECK_FAIL_RETURN_STATUS(!keepAliveTableName_.empty(), K_INVALID, "Coordinator keepalive table is empty");
    CHECK_FAIL_RETURN_STATUS(!keepAliveKey_.empty(), K_INVALID, "Coordinator keepalive key is empty");
    CHECK_FAIL_RETURN_STATUS(keepAliveValue_.lifecycleState != MemberLifecycleState::UNKNOWN, K_INVALID,
                             "Node state should not be empty.");

    MembershipValue value;
    int64_t timeStamp = std::chrono::system_clock::now().time_since_epoch().count();
    {
        std::lock_guard<std::mutex> lock(keepAliveMutex_);
        keepAliveValue_.timestamp = timeStamp;
        value = keepAliveValue_;
    }
    std::string valueStr;
    RETURN_IF_NOT_OK(MembershipValueCodec::Encode(value, valueStr));
    int64_t version = 0;
    int64_t revision = 0;
    auto rc = proxy_->Put(BuildRealKey(keepAliveTableName_, keepAliveKey_), valueStr, keepAliveTtlMs_,
                          COORDINATOR_NO_VERSION_CHECK, version, revision);
    LOG(INFO) << "AutoCreateKeepAliveKey: Put keepalive key " << keepAliveKey_ << " result: " << rc.ToString();
    RETURN_IF_NOT_OK(rc);
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
    return proxy_->KeepAlive(BuildRealKey(keepAliveTableName_, keepAliveKey_), ttlMs, remainingTtlMs);
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
    keepAliveTimeout_ = false;
    state.confirmTimes = 0;
    state.needHandleFailure = true;
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
        state.needHandleFailure = false;
        LOG(WARNING) << "Confirmed local Coordinator network isolation; keep the process alive and report the "
                        "membership deletion event.";
    } else if (status.GetCode() == K_NOT_FOUND) {
        (void)AutoCreateKeepAliveKey();
    }
}

void DsCoordinationBackend::HandleKeepAliveFailed(const std::string &realKey)
{
    CoordinationEvent event;
    event.type = CoordinationEventType::DELETE;
    event.key = realKey;
    event.value = "";
    HandleWatchEvent(std::move(event));
}

void DsCoordinationBackend::CancelWatches()
{
    std::vector<int64_t> watchIds;
    {
        std::lock_guard<std::mutex> lock(watchMutex_);
        watchIds.swap(watchIds_);
        watchedKeys_.clear();
    }
    if (proxy_ == nullptr || watchIds.empty()) {
        return;
    }
    LOG_IF_ERROR(proxy_->CancelWatch(watcherAddr_, watchIds), "Cancel coordinator watches failed");
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
    {
        std::lock_guard<std::mutex> lock(eventHandlerMutex_);
        eventHandler_ = {};
    }
    ShutdownKeepAliveThread();
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
    MembershipValue value;
    {
        std::lock_guard<std::mutex> lock(keepAliveMutex_);
        keepAliveValue_.lifecycleState = state;
        value = keepAliveValue_;
    }
    std::string valueStr;
    RETURN_IF_NOT_OK(MembershipValueCodec::Encode(value, valueStr));
    int64_t version = 0;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(proxy_->Put(BuildRealKey(keepAliveTableName_, keepAliveKey_), valueStr, keepAliveTtlMs_,
                                 COORDINATOR_NO_VERSION_CHECK, version, revision));
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
    std::string valueStr;
    RETURN_IF_NOT_OK(Get(keepAliveTableName_, workerAddr.ToString(), valueStr));
    MembershipValue value;
    RETURN_IF_NOT_OK(MembershipValueCodec::Decode(valueStr, value));
    if (value.lifecycleState == MemberLifecycleState::RESTARTING
        || value.lifecycleState == MemberLifecycleState::RECOVERING) {
        value.lifecycleState = MemberLifecycleState::READY;
        std::string readyValue;
        RETURN_IF_NOT_OK(MembershipValueCodec::Encode(value, readyValue));
        int64_t version = 0;
        int64_t revision = 0;
        RETURN_IF_NOT_OK(proxy_->Put(BuildRealKey(keepAliveTableName_, workerAddr.ToString()), readyValue,
                                     keepAliveTtlMs_, COORDINATOR_NO_VERSION_CHECK, version, revision));
    }
    return Status::OK();
}

bool DsCoordinationBackend::IsKeepAliveTimeout()
{
    return keepAliveTimeout_;
}

bool DsCoordinationBackend::IsFirstKeepAliveSent()
{
    std::lock_guard<std::mutex> lock(keepAliveMutex_);
    return keepAliveValue_.lifecycleState == MemberLifecycleState::RECOVERING;
}

void DsCoordinationBackend::SetEventHandler(EventHandler &&eventHandler)
{
    std::lock_guard<std::mutex> lock(eventHandlerMutex_);
    eventHandler_ = std::move(eventHandler);
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

void DsCoordinationBackend::HandleWatchEvent(CoordinationEvent &&event)
{
    if (!AcceptsWatchEvent(event.key)) {
        return;
    }
    EventHandler handler;
    {
        std::lock_guard<std::mutex> lock(eventHandlerMutex_);
        handler = eventHandler_;
        if (handler == nullptr) {
            return;
        }
        ++activeEventHandlers_;
    }
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

bool DsCoordinationBackend::AcceptsWatchEvent(const std::string &key)
{
    std::lock_guard<std::mutex> lock(watchMutex_);
    return std::any_of(watchedKeys_.begin(), watchedKeys_.end(), [&key](const WatchedKey &watchedKey) {
        if (watchedKey.isPrefix) {
            return key.rfind(watchedKey.key, 0) == 0;
        }
        return key == watchedKey.key;
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
