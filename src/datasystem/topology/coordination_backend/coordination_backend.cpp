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
 * Description: Topology coordination backend event model.
 */
#include "datasystem/topology/coordination_backend/coordination_backend.h"

#include <sstream>

#include "datasystem/common/log/spdlog/provider.h"
#include "datasystem/worker/cluster_manager/cluster_constants.h"

DS_DECLARE_bool(auto_del_dead_node);
DS_DECLARE_string(host_id_env_name);
DS_DECLARE_string(log_dir);
DS_DECLARE_uint32(node_dead_timeout_s);
DS_DECLARE_uint32(node_timeout_s);

namespace datasystem {
namespace topology {
std::string CoordinationEvent::ToString() const
{
    std::stringstream ss;
    ss << "type: " << (type == CoordinationEventType::PUT ? "PUT" : "DELETE") << ", key: " << key
       << ", value: " << value << ", version: " << version << ", revision: " << revision;
    return ss.str();
}

CoordinationBackend::CoordinationBackend(ICoordinatorServiceProxy *proxy, std::string watcherAddr)
    : proxy_(proxy), watcherAddr_(std::move(watcherAddr))
{
    tablePrefixes_.emplace(HASHRING_TABLE, HASHRING_TABLE);
    tablePrefixes_.emplace(CLUSTER_TABLE, "/" + std::string(CLUSTER_TABLE));
    tablePrefixes_.emplace(MASTER_ADDRESS_TABLE, MASTER_ADDRESS_TABLE);
}

CoordinationBackend::~CoordinationBackend()
{
    ShutdownKeepAliveThread();
    CancelWatches();
}

Status CoordinationBackend::GetAll(const std::string &tableName,
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

Status CoordinationBackend::Get(const std::string &tableName, const std::string &key, std::string &value)
{
    RangeSearchResult res;
    RETURN_IF_NOT_OK(Get(tableName, key, res));
    value = std::move(res.value);
    return Status::OK();
}

Status CoordinationBackend::Get(const std::string &tableName, const std::string &key, RangeSearchResult &res,
                                int32_t timeoutMs)
{
    (void)timeoutMs;
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    std::vector<KeyValueEntry> kvs;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(proxy_->Range(BuildRealKey(tableName, key), "", kvs, revision));
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

Status CoordinationBackend::CAS(const std::string &tableName, const std::string &key,
                                const ProcessFunction &processFunc, RangeSearchResult &res)
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    CHECK_FAIL_RETURN_STATUS(processFunc != nullptr, K_INVALID, "Coordinator process function resolve error.");
    const std::string realKey = BuildRealKey(tableName, key);
    int64_t version = 0;
    int64_t revision = 0;
    std::string oldValueFromCas;
    auto coordinatorProcessFunc = [&processFunc, &oldValueFromCas](const std::string &oldValue, std::string &newValue,
                                                                   bool &retry) -> Status {
        oldValueFromCas = oldValue;
        std::unique_ptr<std::string> out;
        RETURN_IF_NOT_OK(processFunc(oldValue, out, retry));
        if (out != nullptr) {
            newValue = std::move(*out);
        }
        return Status::OK();
    };
    RETURN_IF_NOT_OK(proxy_->CAS(realKey, coordinatorProcessFunc, version, revision));
    res.key = realKey;
    res.value = std::move(oldValueFromCas);
    res.version = version;
    res.modRevision = revision;
    return Status::OK();
}

Status CoordinationBackend::CAS(const std::string &tableName, const std::string &key,
                                const ProcessFunction &processFunc)
{
    RangeSearchResult res;
    return CAS(tableName, key, processFunc, res);
}

Status CoordinationBackend::CAS(const std::string &tableName, const std::string &key, const std::string &oldValue,
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

Status CoordinationBackend::Delete(const std::string &tableName, const std::string &key)
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    int64_t deleted = 0;
    int64_t revision = 0;
    return proxy_->DeleteRange(BuildRealKey(tableName, key), "", deleted, revision);
}

Status CoordinationBackend::WatchEvents(const std::vector<WatchKey> &watchKeys)
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    for (const auto &watchKey : watchKeys) {
        const std::string realKey = BuildRealKey(watchKey.tableName, watchKey.key);
        std::vector<KeyValueEntry> initialKvs;
        int64_t watchId = 0;
        RETURN_IF_NOT_OK(proxy_->WatchRange(realKey, StringPlusOne(realKey), watcherAddr_, watchId, initialKvs));
        watchIds_.emplace_back(watchId);
        for (const auto &kv : initialKvs) {
            CoordinationEvent event;
            event.type = CoordinationEventType::PUT;
            event.key = kv.key;
            event.value = kv.value;
            event.version = kv.version;
            event.revision = kv.modRevision;
            HandleWatchEvent(std::move(event));
        }
    }
    return Status::OK();
}

Status CoordinationBackend::InitKeepAlive(const std::string &tableName, const std::string &key, bool isRestart,
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
    keepAliveValue_.timestamp = "_";
    keepAliveValue_.hostId = hostId;
    if (!isStoreAvailableWhenStart) {
        keepAliveValue_.state = ETCD_NODE_DOWNGRADE_RESTART;
    } else if (isRestart) {
        keepAliveValue_.state = "restart";
    } else {
        keepAliveValue_.state = "start";
    }
    isCreateFirstLease_ = true;
    RETURN_IF_NOT_OK(AutoCreateKeepAliveKey());
    LaunchKeepAliveThread();
    return Status::OK();
}

Status CoordinationBackend::AutoCreateKeepAliveKey()
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    CHECK_FAIL_RETURN_STATUS(!keepAliveTableName_.empty(), K_INVALID, "Coordinator keepalive table is empty");
    CHECK_FAIL_RETURN_STATUS(!keepAliveKey_.empty(), K_INVALID, "Coordinator keepalive key is empty");
    CHECK_FAIL_RETURN_STATUS(!keepAliveValue_.state.empty(), K_INVALID, "Node state should not be empty.");

    KeepAliveValue value;
    int64_t timeStamp = std::chrono::system_clock::now().time_since_epoch().count();
    {
        std::lock_guard<std::mutex> lock(keepAliveMutex_);
        keepAliveValue_.timestamp = std::to_string(timeStamp);
        value = keepAliveValue_;
    }
    int64_t version = 0;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(proxy_->Put(BuildRealKey(keepAliveTableName_, keepAliveKey_), value.ToString(), keepAliveTtlMs_,
                                 COORDINATOR_NO_VERSION_CHECK, version, revision));
    {
        std::lock_guard<std::mutex> lock(keepAliveMutex_);
        if (keepAliveValue_.state == "start" || keepAliveValue_.state == "restart") {
            keepAliveValue_.state = "recover";
        }
    }
    return Status::OK();
}

Status CoordinationBackend::RenewKeepAliveOnce()
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    INJECT_POINT("CoordinationBackend.KeepAlive.returnError");
    int64_t ttlMs = keepAliveTtlMs_;
    int64_t remainingTtlMs = 0;
    return proxy_->KeepAlive(BuildRealKey(keepAliveTableName_, keepAliveKey_), ttlMs, remainingTtlMs);
}

void CoordinationBackend::LaunchKeepAliveThread()
{
    ShutdownKeepAliveThread();
    keepAliveExit_ = false;
    keepAliveThread_ = std::thread(&CoordinationBackend::RunKeepAliveLoop, this);
}

void CoordinationBackend::RunKeepAliveLoop()
{
    int64_t intervalMs = keepAliveTtlMs_ / 3;
    constexpr int64_t maxIntervalMs = 5'000;
    constexpr int64_t minIntervalMs = 100;
    intervalMs = std::max(minIntervalMs, std::min(intervalMs, maxIntervalMs));
    int networkFailedConfirmMinTimes = 3;
    INJECT_POINT("CoordinationBackend.KeepAlive.intervalMs", [&intervalMs](int timeMs) { intervalMs = timeMs; });
    INJECT_POINT("CoordinationBackend.KeepAlive.confirmTimes",
                 [&networkFailedConfirmMinTimes](int times) { networkFailedConfirmMinTimes = times; });
    int networkFailedConfirmTimes = 0;
    bool needHandleKeepAliveFailed = true;
    bool deathTimerStarted = false;
    Timer keepAliveTimeoutTimer;
    Timer deathTimer;
    const std::string realKey = BuildRealKey(keepAliveTableName_, keepAliveKey_);
    while (!keepAliveExit_) {
        auto rc = RenewKeepAliveOnce();
        if (rc.IsOk()) {
            keepAliveTimeout_ = false;
            keepAliveTimeoutTimer.Reset();
            deathTimer.Clear();
            deathTimerStarted = false;
            networkFailedConfirmTimes = 0;
            needHandleKeepAliveFailed = true;
        } else {
            keepAliveTimeout_ = true;
            bool storeAvailableWhenNetworkFailed = false;
            if (needHandleKeepAliveFailed && checkStoreStateWhenNetworkFailedHandler_ != nullptr) {
                storeAvailableWhenNetworkFailed = checkStoreStateWhenNetworkFailedHandler_();
                if (!storeAvailableWhenNetworkFailed) {
                    keepAliveTimeoutTimer.Reset();
                    networkFailedConfirmTimes = 0;
                }
            }
            if (storeAvailableWhenNetworkFailed && ++networkFailedConfirmTimes >= networkFailedConfirmMinTimes) {
                HandleKeepAliveFailed(realKey);
                needHandleKeepAliveFailed = false;
                constexpr int64_t killGuardDelaySec = 10;
                auto deathTimeoutMs = (static_cast<int64_t>(FLAGS_node_dead_timeout_s)
                                       - static_cast<int64_t>(FLAGS_node_timeout_s) + killGuardDelaySec)
                                      * MS_PER_SECOND;
                INJECT_POINT_NO_RETURN("CoordinationBackend.KeepAlive.deathTimeoutMs",
                                       [&deathTimeoutMs](int64_t timeoutMs) { deathTimeoutMs = timeoutMs; });
                deathTimer.AdjustTimeoutAndReset(deathTimeoutMs);
                deathTimerStarted = true;
            } else if (deathTimerStarted && deathTimer.IsTimeout()) {
                (void)KillLocalWorkerForKeepAliveFailure();
            } else if (rc.GetCode() == K_NOT_FOUND) {
                (void)AutoCreateKeepAliveKey();
            }
            if (storeAvailableWhenNetworkFailed
                && keepAliveTimeoutTimer.ElapsedMilliSecond()
                       > static_cast<double>(FLAGS_node_dead_timeout_s * MS_PER_SECOND)
                && FLAGS_auto_del_dead_node) {
                (void)KillLocalWorkerForKeepAliveFailure();
            }
        }
        std::unique_lock<std::mutex> lock(keepAliveMutex_);
        keepAliveCv_.wait_for(lock, std::chrono::milliseconds(intervalMs), [this]() { return keepAliveExit_.load(); });
    }
}

void CoordinationBackend::HandleKeepAliveFailed(const std::string &realKey)
{
    CoordinationEvent event;
    event.type = CoordinationEventType::DELETE;
    event.key = realKey;
    event.value = "";
    HandleWatchEvent(std::move(event));
}

Status CoordinationBackend::KillLocalWorkerForKeepAliveFailure()
{
    INJECT_POINT("CoordinationBackend.KeepAlive.kill");
    LOG(WARNING) << "Coordinator keepalive failed long enough; kill local worker for passive scale down.";
    Provider::Instance().FlushLogs();
    (void)raise(SIGKILL);
    return Status::OK();
}

void CoordinationBackend::CancelWatches()
{
    if (proxy_ == nullptr || watchIds_.empty()) {
        return;
    }
    LOG_IF_ERROR(proxy_->CancelWatch(watcherAddr_, watchIds_), "Cancel coordinator watches failed");
    watchIds_.clear();
}

void CoordinationBackend::ShutdownKeepAliveThread()
{
    keepAliveExit_ = true;
    keepAliveCv_.notify_all();
    if (keepAliveThread_.joinable()) {
        keepAliveThread_.join();
    }
}

Status CoordinationBackend::UpdateNodeState(const std::string &state)
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    CHECK_FAIL_RETURN_STATUS(!IsKeepAliveTimeout(), K_NOT_READY,
                             "The key written to the cluster table must be bound to a lease");
    KeepAliveValue value;
    {
        std::lock_guard<std::mutex> lock(keepAliveMutex_);
        keepAliveValue_.state = state;
        value = keepAliveValue_;
    }
    int64_t version = 0;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(proxy_->Put(BuildRealKey(keepAliveTableName_, keepAliveKey_), value.ToString(), keepAliveTtlMs_,
                                 COORDINATOR_NO_VERSION_CHECK, version, revision));
    isCreateFirstLease_ = false;
    return Status::OK();
}

Status CoordinationBackend::GetStorePrefix(const std::string &tableName, std::string &prefix)
{
    auto iter = tablePrefixes_.find(tableName);
    CHECK_FAIL_RETURN_STATUS(iter != tablePrefixes_.end(), K_RUNTIME_ERROR,
                             "The table does not exist. tableName:" + tableName);
    prefix = iter->second;
    return Status::OK();
}

Status CoordinationBackend::InformReconciliationDone(const HostPort &workerAddr)
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    std::string valueStr;
    RETURN_IF_NOT_OK(Get(CLUSTER_TABLE, workerAddr.ToString(), valueStr));
    KeepAliveValue value;
    RETURN_IF_NOT_OK(KeepAliveValue::FromString(valueStr, value));
    if (value.state == "restart" || value.state == "recover") {
        value.state = ETCD_NODE_READY;
        int64_t version = 0;
        int64_t revision = 0;
        RETURN_IF_NOT_OK(proxy_->Put(BuildRealKey(CLUSTER_TABLE, workerAddr.ToString()), value.ToString(),
                                     keepAliveTtlMs_, COORDINATOR_NO_VERSION_CHECK, version, revision));
    }
    return Status::OK();
}

bool CoordinationBackend::IsKeepAliveTimeout()
{
    return keepAliveTimeout_;
}

bool CoordinationBackend::IsCreateFirstLease()
{
    return isCreateFirstLease_;
}

void CoordinationBackend::SetEventHandler(EventHandler &&eventHandler)
{
    std::lock_guard<std::mutex> lock(eventHandlerMutex_);
    eventHandler_ = std::move(eventHandler);
}

void CoordinationBackend::SetCheckStoreStateWhenNetworkFailedHandler(std::function<bool()> handler)
{
    checkStoreStateWhenNetworkFailedHandler_ = std::move(handler);
}

const std::string &CoordinationBackend::GetWatcherAddr() const
{
    return watcherAddr_;
}

void CoordinationBackend::HandleWatchEvent(CoordinationEvent &&event)
{
    EventHandler handler;
    {
        std::lock_guard<std::mutex> lock(eventHandlerMutex_);
        handler = eventHandler_;
    }
    if (handler) {
        handler(std::move(event));
    }
}

std::string CoordinationBackend::RemoveTablePrefix(const std::string &key, const std::string &prefix)
{
    const std::string prefixWithSlash = prefix + "/";
    if (key.rfind(prefixWithSlash, 0) == 0) {
        return key.substr(prefixWithSlash.size());
    }
    return key;
}

std::string CoordinationBackend::BuildRealKey(const std::string &tableName, const std::string &key)
{
    std::string prefix;
    Status status = GetStorePrefix(tableName, prefix);
    if (status.IsError()) {
        return key;
    }
    return prefix + "/" + key;
}
}  // namespace topology
}  // namespace datasystem
