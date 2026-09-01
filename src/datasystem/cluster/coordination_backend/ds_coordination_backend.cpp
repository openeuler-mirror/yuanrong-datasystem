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
#include <array>
#include <exception>
#include <thread>

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
#include "butil/time.h"

DS_DECLARE_string(host_id_env_name);
DS_DECLARE_string(log_dir);
DS_DECLARE_uint32(node_dead_timeout_s);
DS_DECLARE_uint32(node_timeout_s);

namespace datasystem::cluster {
namespace {
constexpr size_t COORDINATOR_ID_LOG_PREFIX_SIZE = 8;
constexpr int64_t KEEP_ALIVE_INTERVAL_DIVISOR = 3;
constexpr int64_t PEER_RPC_FAILURE_WINDOW_DIVISOR = 2;
constexpr uint64_t MIN_PEER_RPC_FAILURES_TO_REPORT = 3;
constexpr int64_t MIN_INITIAL_KEEPALIVE_RETRY_MS = 10'000;
constexpr int64_t INITIAL_KEEPALIVE_RETRY_INTERVAL_MS = 200;
constexpr int64_t MEMBERSHIP_MUTATION_SLOW_HOLD_MS = DEFAULT_COORDINATOR_RPC_TIMEOUT_MS;
constexpr uint32_t MEMBERSHIP_MUTATION_SLOW_LOG_EVERY_N = 100;
constexpr uint32_t MEMBERSHIP_MUTATION_DIAGNOSTIC_READ_RETRIES = 3;
}

struct DsCoordinationBackend::KeepAliveFailureState {
    int confirmMinTimes{ 3 };
    int confirmTimes{ 0 };
    bool needHandleFailure{ true };
};

DsCoordinationBackend::MembershipMutationGuard::MembershipMutationGuard(
    DsCoordinationBackend &backend, MembershipMutationOperation operation)
    : backend_(backend), operation_(operation), lock_(backend_.membershipMutationMutex_),
      acquiredAt_(std::chrono::steady_clock::now())
{
    backend_.RecordMembershipMutationAcquired(operation_, acquiredAt_);
}

DsCoordinationBackend::MembershipMutationGuard::MembershipMutationGuard(
    DsCoordinationBackend &backend, MembershipMutationOperation operation, std::adopt_lock_t)
    : backend_(backend), operation_(operation), lock_(backend_.membershipMutationMutex_, std::adopt_lock),
      acquiredAt_(std::chrono::steady_clock::now())
{
    backend_.RecordMembershipMutationAcquired(operation_, acquiredAt_);
}

DsCoordinationBackend::MembershipMutationGuard::~MembershipMutationGuard()
{
    const auto heldMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - acquiredAt_)
                            .count();
    const auto phase = backend_.ClearMembershipMutationOwner();
    lock_.unlock();
    if (heldMs >= MEMBERSHIP_MUTATION_SLOW_HOLD_MS) {
        LOG_FIRST_EVERY_N(WARNING, MEMBERSHIP_MUTATION_SLOW_LOG_EVERY_N)
            << "event=CLUSTER_MEMBERSHIP_MUTATION action=slow_hold owner="
            << DsCoordinationBackend::MembershipMutationOperationName(operation_)
            << " phase=" << DsCoordinationBackend::MembershipMutationPhaseName(phase) << " held_ms=" << heldMs;
    }
}

void DsCoordinationBackend::MembershipMutationGuard::SetPhase(MembershipMutationPhase phase)
{
    backend_.RecordMembershipMutationPhase(phase);
}

void DsCoordinationBackend::RecordMembershipMutationAcquired(
    MembershipMutationOperation operation, std::chrono::steady_clock::time_point acquiredAt)
{
    const auto writeSequence = membershipMutationDiagnosticSequence_.fetch_add(1, std::memory_order_acq_rel) + 1;
    std::atomic_thread_fence(std::memory_order_release);
    INJECT_POINT_NO_RETURN("CoordinationBackend.MembershipMutation.afterDiagnosticWriteBegin");
    membershipMutationOwner_.store(operation, std::memory_order_relaxed);
    membershipMutationPhase_.store(MembershipMutationPhase::ACQUIRED, std::memory_order_relaxed);
    membershipMutationAcquiredAtMs_.store(
        std::chrono::duration_cast<std::chrono::milliseconds>(acquiredAt.time_since_epoch()).count(),
        std::memory_order_relaxed);
    membershipMutationDiagnosticSequence_.store(writeSequence + 1, std::memory_order_release);
    INJECT_POINT_NO_RETURN("CoordinationBackend.MembershipMutation.afterDiagnosticAcquired");
}

void DsCoordinationBackend::RecordMembershipMutationPhase(MembershipMutationPhase phase)
{
    membershipMutationPhase_.store(phase, std::memory_order_release);
}

DsCoordinationBackend::MembershipMutationPhase DsCoordinationBackend::ClearMembershipMutationOwner()
{
    const auto writeSequence = membershipMutationDiagnosticSequence_.fetch_add(1, std::memory_order_acq_rel) + 1;
    std::atomic_thread_fence(std::memory_order_release);
    const auto phase = membershipMutationPhase_.load(std::memory_order_relaxed);
    membershipMutationOwner_.store(MembershipMutationOperation::NONE, std::memory_order_relaxed);
    membershipMutationPhase_.store(MembershipMutationPhase::NONE, std::memory_order_relaxed);
    membershipMutationAcquiredAtMs_.store(0, std::memory_order_relaxed);
    membershipMutationDiagnosticSequence_.store(writeSequence + 1, std::memory_order_release);
    return phase;
}

DsCoordinationBackend::MembershipMutationDiagnostic DsCoordinationBackend::ReadMembershipMutationDiagnostic() const
{
    for (uint32_t retry = 0; retry < MEMBERSHIP_MUTATION_DIAGNOSTIC_READ_RETRIES; ++retry) {
        const auto sequenceBefore = membershipMutationDiagnosticSequence_.load(std::memory_order_acquire);
        if ((sequenceBefore & 1U) != 0) {
            continue;
        }
        MembershipMutationDiagnosticSnapshot snapshot{
            membershipMutationOwner_.load(std::memory_order_relaxed),
            membershipMutationPhase_.load(std::memory_order_acquire),
            membershipMutationAcquiredAtMs_.load(std::memory_order_relaxed)
        };
        INJECT_POINT_NO_RETURN("CoordinationBackend.MembershipMutation.beforeDiagnosticSequenceValidation");
        std::atomic_thread_fence(std::memory_order_acquire);
        if (sequenceBefore == membershipMutationDiagnosticSequence_.load(std::memory_order_relaxed)) {
            return snapshot;
        }
    }
    return std::nullopt;
}

std::string DsCoordinationBackend::GetMembershipMutationDiagnostic(
    MembershipMutationOperation waiter, std::chrono::steady_clock::time_point waitStartedAt) const
{
    const auto snapshot = ReadMembershipMutationDiagnostic();
    const auto now = std::chrono::steady_clock::now();
    const auto waitMs = std::chrono::duration_cast<std::chrono::milliseconds>(now - waitStartedAt).count();
    const auto prefix = "waiter=" + std::string(MembershipMutationOperationName(waiter)) + ", wait_ms="
                        + std::to_string(waitMs);
    if (!snapshot.has_value()) {
        return prefix + ", owner=changing";
    }
    if (snapshot->owner == MembershipMutationOperation::NONE) {
        return prefix + ", owner=unknown";
    }
    const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    const auto heldMs = nowMs - snapshot->acquiredAtMs;
    return prefix + ", owner=" + MembershipMutationOperationName(snapshot->owner)
           + ", owner_phase=" + MembershipMutationPhaseName(snapshot->phase) + ", held_ms=" + std::to_string(heldMs);
}

const char *DsCoordinationBackend::MembershipMutationOperationName(MembershipMutationOperation operation)
{
    static constexpr std::array<const char *, static_cast<size_t>(MembershipMutationOperation::COUNT)> names{
        "none",
        "delete_membership",
        "create_keepalive",
        "recreate_keepalive",
        "renew_keepalive",
        "mark_exiting",
        "update_membership_state",
        "inform_reconciliation_done",
        "install_ensured_membership",
        "ensure_membership"
    };
    return names[static_cast<size_t>(operation)];
}

const char *DsCoordinationBackend::MembershipMutationPhaseName(MembershipMutationPhase phase)
{
    static constexpr std::array<const char *, static_cast<size_t>(MembershipMutationPhase::COUNT)> names{
        "none",
        "acquired",
        "prepare_payload",
        "update_local_membership",
        "install_revision"
    };
    return names[static_cast<size_t>(phase)];
}

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

Status DsCoordinationBackend::GetIfChanged(const std::string &tableName, const std::string &key,
                                           int64_t knownModRevision, RangeSearchResult &res, bool &unchanged,
                                           int32_t timeoutMs)
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    CHECK_FAIL_RETURN_STATUS(knownModRevision > 0, K_INVALID, "known modification revision must be positive");
    std::vector<KeyValueEntry> kvs;
    int64_t revision = 0;
    auto rc =
        proxy_->RangeIfChanged(BuildRealKey(tableName, key), knownModRevision, kvs, revision, unchanged, timeoutMs);
    RefreshWatchIdentity(rc);
    RETURN_IF_NOT_OK(rc);
    if (unchanged) {
        CHECK_FAIL_RETURN_STATUS(kvs.empty(), K_RUNTIME_ERROR, "unchanged Coordinator Range returned values");
        return Status::OK();
    }
    if (kvs.empty()) {
        RETURN_STATUS(K_NOT_FOUND, "The key does not exist in coordinator. key:" + key);
    }
    CHECK_FAIL_RETURN_STATUS(kvs.size() == 1, K_KVSTORE_ERROR, "Coordinator key value is not unique. key:" + key);
    res.key = kvs.front().key;
    res.value = std::move(kvs.front().value);
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
    const bool deletesLocalMembership = tableName == keepAliveTableName_ && key == keepAliveKey_;
    MembershipIncarnation startedFrom;
    if (deletesLocalMembership) {
        MembershipMutationGuard mutationGuard(*this, MembershipMutationOperation::DELETE_MEMBERSHIP);
        mutationGuard.SetPhase(MembershipMutationPhase::PREPARE_PAYLOAD);
        startedFrom = GetMembershipIncarnationLocked();
    }
    const int64_t expectedModRevision =
        deletesLocalMembership ? startedFrom.modRevision : COORDINATOR_NO_MOD_REVISION_CHECK;
    const auto realKey = BuildRealKey(tableName, key);
    auto rc = deletesLocalMembership
                  ? proxy_->DeleteMembership(realKey, deleted, revision, timeoutMs, startedFrom.coordinatorId,
                                             expectedModRevision)
                  : proxy_->DeleteRange(realKey, "", deleted, revision, timeoutMs, expectedModRevision);
    RefreshWatchIdentity(rc);
    if (rc.IsError() || !deletesLocalMembership) {
        return rc;
    }
    MembershipMutationGuard mutationGuard(*this, MembershipMutationOperation::DELETE_MEMBERSHIP);
    mutationGuard.SetPhase(MembershipMutationPhase::UPDATE_LOCAL_MEMBERSHIP);
    if (!IsMembershipIncarnationCurrentLocked(startedFrom)) {
        RETURN_STATUS(K_TRY_AGAIN, "membership changed while deletion was in flight");
    }
    keepAliveModRevision_ = COORDINATOR_NO_MOD_REVISION_CHECK;
    return Status::OK();
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
    RETURN_IF_NOT_OK(
        PrepareWatchPlan(watchKeys, registrations, registeredIds, initialEvents, batchCoordinatorId));
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
        auto rc = proxy_->WatchRange(realKey, rangeEnd, watcherAddr_, pendingWatchRegistrationId_ + realKey,
                                     watchId, initialKvs,
                                     DEFAULT_COORDINATOR_RPC_TIMEOUT_MS, &responseCoordinatorId);
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
            initialEvents.push_back({ CoordinationEventType::PUT, std::move(kv.key), std::move(kv.value),
                                      kv.version, kv.modRevision, responseCoordinatorId, watchId });
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
              << ", coordinator_id="
              << BytesUuidToString(coordinatorId).substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE);
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

std::chrono::milliseconds DsCoordinationBackend::GetIdentityProbeBackoffLimit(
    std::chrono::steady_clock::time_point now)
{
    if (identityProbeFailureStartedAt_ == std::chrono::steady_clock::time_point()) {
        identityProbeFailureStartedAt_ = now;
    }
    const auto failureDuration = now - identityProbeFailureStartedAt_;
    const auto growthWindows = failureDuration / IDENTITY_PROBE_BACKOFF_GROWTH_WINDOW;
    auto limit = INITIAL_IDENTITY_PROBE_BACKOFF_LIMIT;
    for (int64_t index = 0; index < std::min<int64_t>(growthWindows, 3); ++index) {
        limit = std::min(limit * IDENTITY_PROBE_BACKOFF_MULTIPLIER, MAX_IDENTITY_PROBE_BACKOFF);
    }
    return limit;
}

void DsCoordinationBackend::RefreshWatchIdentity(const Status &status)
{
    std::string coordinatorId;
    const bool probe = status.GetCode() == K_NOT_READY || IsRetryableRpcError(status) || IsNonRetryableRpcError(status);
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
        }
        if (proxy_->GetCoordinatorId(coordinatorId).IsError()) {
            std::lock_guard<std::mutex> lock(watchMutex_);
            const auto probeCompletedAt = std::chrono::steady_clock::now();
            nextIdentityProbeAt_ = probeCompletedAt + identityProbeBackoff_;
            identityProbeBackoff_ =
                std::min(identityProbeBackoff_ * IDENTITY_PROBE_BACKOFF_MULTIPLIER,
                         GetIdentityProbeBackoffLimit(probeCompletedAt));
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
            identityProbeFailureStartedAt_ = {};
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

Status DsCoordinationBackend::PutWithKeepAliveLease(const std::string &tableName, const std::string &key,
                                                    const std::string &value)
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    CHECK_FAIL_RETURN_STATUS(!IsKeepAliveTimeout() && keepAliveTtlMs_ > 0, K_NOT_READY,
                             "UB health value must be bound to an active membership lease");
    int64_t version = 0;
    int64_t revision = 0;
    return proxy_->Put(BuildRealKey(tableName, key), value, keepAliveTtlMs_, COORDINATOR_NO_VERSION_CHECK, version,
                       revision);
}

std::string DsCoordinationBackend::ResolveKeepAliveHostId() const
{
    std::string hostId;
    if (FLAGS_host_id_env_name.empty()) {
        // host_id_env_name is unset: the worker registers an empty host_id, so clients cannot partition
        // same-node workers and sdk_data_placement_policy=PREFERRED_SAME_NODE silently degrades to the
        // hash ring (cross-node routing for every key, including large payloads that may time out).
        LOG(WARNING) << "host_id_env_name is not set; worker will register an empty host_id and same-node "
                        "worker affinity (sdk_data_placement_policy=PREFERRED_SAME_NODE) will be disabled. "
                        "Set --host_id_env_name=<ENV_VAR> and export <ENV_VAR>=<unique-per-host value> on each "
                        "worker host to enable same-node routing.";
        return hostId;
    }
    auto envHostId = GetStringFromEnv(FLAGS_host_id_env_name.c_str(), "");
    auto envFilePath = GetWorkerEnvFilePath(FLAGS_log_dir);
    hostId = GetStringFromEnvOrFile(FLAGS_host_id_env_name.c_str(), envFilePath, FLAGS_host_id_env_name, "");
    if (hostId.empty()) {
        LOG(WARNING) << FormatString(
            "host_id env [%s] is empty when worker registers to coordinator. "
            "Check --host_id_env_name config or the worker env file in --log_dir.",
            FLAGS_host_id_env_name);
    } else if (envHostId.empty()) {
        LOG(INFO) << "Host id is " << hostId << " from persisted worker env file " << envFilePath;
    } else {
        LOG(INFO) << "Host id is " << hostId << " from env " << FLAGS_host_id_env_name;
    }
    return hostId;
}

Status DsCoordinationBackend::CreateKeepAliveKeyWithRetry()
{
    auto createStatus = AutoCreateKeepAliveKey(true);
    const auto retryBudgetMs =
        std::min<int64_t>(static_cast<int64_t>(FLAGS_node_dead_timeout_s) * MS_PER_SECOND,
                          std::max<int64_t>(MIN_INITIAL_KEEPALIVE_RETRY_MS,
                                            static_cast<int64_t>(FLAGS_node_timeout_s) * MS_PER_SECOND
                                                * KEEP_ALIVE_INTERVAL_DIVISOR));
    const auto retryDeadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(retryBudgetMs);
    uint32_t retryAttempts = 0;
    while (IsRetryableRpcError(createStatus) && std::chrono::steady_clock::now() < retryDeadline) {
        ++retryAttempts;
        LOG(WARNING) << "CLUSTER_MEMBERSHIP role=worker action=initial_keepalive_retry address=" << watcherAddr_
                     << " attempt=" << retryAttempts << " status=" << createStatus.ToString();
        std::this_thread::sleep_for(std::chrono::milliseconds(INITIAL_KEEPALIVE_RETRY_INTERVAL_MS));
        createStatus = AutoCreateKeepAliveKey(true);
    }
    if (retryAttempts > 0) {
        LOG(INFO) << "CLUSTER_MEMBERSHIP role=worker action=initial_keepalive_retry_finished address=" << watcherAddr_
                  << " attempts=" << retryAttempts << " status=" << createStatus.ToString();
    }
    return createStatus;
}

Status DsCoordinationBackend::InitKeepAlive(const std::string &tableName, const std::string &key, bool isRestart,
                                            bool isStoreAvailableWhenStart)
{
    auto hostId = ResolveKeepAliveHostId();
    keepAliveTableName_ = tableName;
    keepAliveKey_ = key;
    firstKeepAliveSent_.store(false, std::memory_order_release);
    exitMembershipRequested_.store(false, std::memory_order_release);
    keepAliveTtlMs_ = static_cast<int64_t>(FLAGS_node_timeout_s) * MS_PER_SECOND;
    keepAliveIntervalMs_ = static_cast<int64_t>(FLAGS_node_timeout_s) * MS_PER_SECOND / KEEP_ALIVE_INTERVAL_DIVISOR;
    // Keep one process incarnation across ambiguous lease-publication retries.
    keepAliveValue_.timestamp = std::chrono::system_clock::now().time_since_epoch().count();
    keepAliveValue_.hostId = hostId;
    keepAliveValue_.compatibilityVersion = CompatibilityManager::Instance().GetCurrentCompatibilityVersion().ToString();
    keepAliveValue_.lifecycleState = !isStoreAvailableWhenStart ? MemberLifecycleState::DOWNGRADE_RESTARTING
                                     : isRestart                ? MemberLifecycleState::RESTARTING
                                                                : MemberLifecycleState::STARTING;
    // Publishing the lease can race the previous lease's TTL delete, which also removes this address's watch channels.
    auto createStatus = CreateKeepAliveKeyWithRetry();
    MembershipReconcileHandler reconcile;
    if (createStatus.GetCode() == K_NOT_READY) {
        std::lock_guard<std::mutex> lock(eventHandlerMutex_);
        reconcile = membershipReconcileHandler_;
    }
    if (createStatus.GetCode() == K_NOT_READY) {
        if (reconcile != nullptr) {
            RETURN_IF_NOT_OK(reconcile(true));
        } else {
            RETURN_IF_NOT_OK(createStatus);
        }
    } else {
        RETURN_IF_NOT_OK(createStatus);
    }
    LaunchKeepAliveThread();
    return Status::OK();
}

Status DsCoordinationBackend::AutoCreateKeepAliveKey(bool recreated)
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    CHECK_FAIL_RETURN_STATUS(!keepAliveTableName_.empty(), K_INVALID, "Coordinator keepalive table is empty");
    CHECK_FAIL_RETURN_STATUS(!keepAliveKey_.empty(), K_INVALID, "Coordinator keepalive key is empty");
    MembershipRecreateGate recreateGate;
    if (recreated) {
        std::lock_guard<std::mutex> lock(eventHandlerMutex_);
        recreateGate = membershipRecreateGate_;
    }
    if (recreateGate != nullptr) {
        RETURN_IF_NOT_OK(recreateGate());
    }
    MembershipIncarnation startedFrom;
    {
        const auto operation = recreated ? MembershipMutationOperation::RECREATE_KEEPALIVE
                                         : MembershipMutationOperation::CREATE_KEEPALIVE;
        MembershipMutationGuard mutationGuard(*this, operation);
        mutationGuard.SetPhase(MembershipMutationPhase::PREPARE_PAYLOAD);
        startedFrom = GetMembershipIncarnationLocked();
    }
    const std::string physicalKey = BuildRealKey(keepAliveTableName_, keepAliveKey_);
    std::vector<KeyValueEntry> current;
    int64_t rangeRevision = 0;
    std::string rangeCoordinatorId;
    auto rangeStatus = proxy_->Range(physicalKey, "", current, rangeRevision, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS,
                                     &rangeCoordinatorId);
    RETURN_IF_NOT_OK(rangeStatus);
    CHECK_FAIL_RETURN_STATUS(current.size() <= 1, K_RUNTIME_ERROR,
                             "membership exact read returned multiple values");
    const int64_t expectedVersion =
        current.empty() ? COORDINATOR_KEY_NOT_EXISTS_VERSION : current.front().version;
    const int64_t expectedModRevision =
        current.empty() ? COORDINATOR_NO_MOD_REVISION_CHECK : current.front().modRevision;
    MembershipValue publishedValue;
    std::string valueStr;
    RETURN_IF_NOT_OK(
        EncodeMembershipForRecreation(current, startedFrom, rangeCoordinatorId, publishedValue, valueStr));
    int64_t version = 0;
    int64_t revision = 0;
    std::string coordinatorId;
    auto rc = proxy_->Put(physicalKey, valueStr, keepAliveTtlMs_, expectedVersion, version, revision,
                          DEFAULT_COORDINATOR_RPC_TIMEOUT_MS, &coordinatorId, rangeCoordinatorId,
                          expectedModRevision);
    LOG(INFO) << "AutoCreateKeepAliveKey: Put keepalive key " << keepAliveKey_ << " result: " << rc.ToString();
    RETURN_IF_NOT_OK(rc);
    return CommitCreatedMembership(startedFrom, coordinatorId, revision, publishedValue, recreated);
}

Status DsCoordinationBackend::EncodeMembershipForRecreation(const std::vector<KeyValueEntry> &current,
                                                            const MembershipIncarnation &startedFrom,
                                                            const std::string &rangeCoordinatorId,
                                                            MembershipValue &publishedValue,
                                                            std::string &encodedValue)
{
    {
        std::lock_guard<std::mutex> lock(keepAliveMutex_);
        CHECK_FAIL_RETURN_STATUS(keepAliveValue_.lifecycleState != MemberLifecycleState::UNKNOWN, K_INVALID,
                                 "Node state should not be empty.");
        publishedValue = keepAliveValue_;
    }
    const bool observedDifferentIncarnation = !current.empty()
                                              && startedFrom.modRevision != COORDINATOR_NO_MOD_REVISION_CHECK
                                              && (rangeCoordinatorId != startedFrom.coordinatorId
                                                  || current.front().modRevision != startedFrom.modRevision);
    if (observedDifferentIncarnation) {
        MembershipValue currentValue;
        RETURN_IF_NOT_OK(MembershipValueCodec::Decode(current.front().value, currentValue));
        CHECK_FAIL_RETURN_STATUS(currentValue.timestamp == publishedValue.timestamp, K_TRY_AGAIN,
                                 "membership process incarnation changed before recreation");
        publishedValue = std::move(currentValue);
    }
    if (exitMembershipRequested_.load(std::memory_order_acquire)) {
        publishedValue.lifecycleState = MemberLifecycleState::EXITING;
    }
    return MembershipValueCodec::Encode(publishedValue, encodedValue);
}

Status DsCoordinationBackend::CommitCreatedMembership(const MembershipIncarnation &startedFrom,
                                                      const std::string &coordinatorId, int64_t revision,
                                                      const MembershipValue &publishedValue, bool recreated)
{
    MembershipCommitResult result;
    uint64_t successEpoch = 0;
    bool repairExit = false;
    bool generationChanged = false;
    {
        const auto operation = recreated ? MembershipMutationOperation::RECREATE_KEEPALIVE
                                         : MembershipMutationOperation::CREATE_KEEPALIVE;
        MembershipMutationGuard mutationGuard(*this, operation);
        mutationGuard.SetPhase(MembershipMutationPhase::UPDATE_LOCAL_MEMBERSHIP);
        result = CommitMembershipRevisionLocked(startedFrom, coordinatorId, revision);
        if (result == MembershipCommitResult::COMMITTED) {
            firstKeepAliveSent_.store(true, std::memory_order_release);
            std::lock_guard<std::mutex> lock(keepAliveMutex_);
            generationChanged = keepAliveValue_.timestamp != publishedValue.timestamp;
            if (!generationChanged) {
                keepAliveValue_ = publishedValue;
                successEpoch = ++membershipSuccessEpoch_;
            }
            if (exitMembershipRequested_.load(std::memory_order_acquire)) {
                keepAliveValue_.lifecycleState = MemberLifecycleState::EXITING;
                repairExit = publishedValue.lifecycleState != MemberLifecycleState::EXITING;
            } else if (!generationChanged
                       && (keepAliveValue_.lifecycleState == MemberLifecycleState::STARTING
                           || keepAliveValue_.lifecycleState == MemberLifecycleState::RESTARTING)) {
                keepAliveValue_.lifecycleState = MemberLifecycleState::RECOVERING;
            }
        }
    }
    CHECK_FAIL_RETURN_STATUS(result != MembershipCommitResult::STALE_LIFETIME, K_TRY_AGAIN,
                             "membership Coordinator changed while recreation was in flight");
    if (result == MembershipCommitResult::COMMITTED && !generationChanged) {
        HandleMembershipSuccess(coordinatorId, successEpoch, recreated);
    }
    if (result == MembershipCommitResult::COMMITTED && repairExit) {
        return UpdateNodeState(MemberLifecycleState::EXITING);
    }
    CHECK_FAIL_RETURN_STATUS(!generationChanged, K_TRY_AGAIN,
                             "membership generation changed while recreation was in flight");
    return Status::OK();
}

Status DsCoordinationBackend::RenewKeepAliveOnce()
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    INJECT_POINT("CoordinationBackend.KeepAlive.returnError");
    MembershipIncarnation startedFrom;
    {
        MembershipMutationGuard mutationGuard(*this, MembershipMutationOperation::RENEW_KEEPALIVE);
        mutationGuard.SetPhase(MembershipMutationPhase::PREPARE_PAYLOAD);
        CHECK_FAIL_RETURN_STATUS(keepAliveModRevision_ != COORDINATOR_NO_MOD_REVISION_CHECK, K_NOT_READY,
                                 "membership incarnation is not established");
        startedFrom = GetMembershipIncarnationLocked();
    }
    int64_t ttlMs = keepAliveTtlMs_;
    int64_t remainingTtlMs = 0;
    std::string coordinatorId;
    const auto failedTargets = GetFailedTargets(std::chrono::steady_clock::now());
    auto rc = proxy_->KeepAlive(BuildRealKey(keepAliveTableName_, keepAliveKey_), ttlMs, remainingTtlMs,
                                DEFAULT_COORDINATOR_RPC_TIMEOUT_MS, &coordinatorId, startedFrom.coordinatorId,
                                startedFrom.modRevision, failedTargets);
    uint64_t successEpoch = 0;
    {
        MembershipMutationGuard mutationGuard(*this, MembershipMutationOperation::RENEW_KEEPALIVE);
        mutationGuard.SetPhase(MembershipMutationPhase::UPDATE_LOCAL_MEMBERSHIP);
        if (!IsMembershipIncarnationCurrentLocked(startedFrom)) {
            return Status::OK();
        }
        if (rc.IsError()) {
            keepAliveTimeout_ = true;
        } else {
            keepAliveTimeout_ = false;
            successEpoch = ++membershipSuccessEpoch_;
        }
    }
    if (rc.IsOk()) {
        HandleMembershipSuccess(coordinatorId, successEpoch);
    }
    return rc;
}

void DsCoordinationBackend::RecordPeerRpcFailure(const HostPort &target)
{
    RecordPeerRpcFailure(target, std::chrono::steady_clock::now());
}

void DsCoordinationBackend::RecordPeerRpcFailure(const HostPort &target, std::chrono::steady_clock::time_point now)
{
    const auto targetAddress = target.ToString();
    if (targetAddress.empty()) {
        return;
    }
    const auto failureWindow =
        std::chrono::milliseconds(static_cast<int64_t>(FLAGS_node_timeout_s) * MS_PER_SECOND
                                  / PEER_RPC_FAILURE_WINDOW_DIVISOR);
    bool shouldWake = false;
    uint64_t failedCount = 0;
    int64_t failedMs = 0;
    {
        std::lock_guard<std::mutex> lock(rpcFailedMutex_);
        auto &state = rpcFailedStates_[targetAddress];
        if (!state.reported && state.failedCount > 0 && now - state.lastFailedAt > failureWindow) {
            state.failedCount = 0;
            state.firstFailedAt = now;
        }
        if (state.failedCount == 0) {
            state.firstFailedAt = now;
        }
        ++state.failedCount;
        state.lastFailedAt = now;
        hasRpcFailures_.store(true, std::memory_order_release);
        if (!state.reported && state.failedCount >= MIN_PEER_RPC_FAILURES_TO_REPORT
            && now - state.firstFailedAt >= failureWindow) {
            state.reported = true;
            immediateReportSignal_ = true;
            shouldWake = true;
        }
        failedCount = state.failedCount;
        failedMs = std::chrono::duration_cast<std::chrono::milliseconds>(now - state.firstFailedAt).count();
    }
    if (shouldWake) {
        LOG(INFO) << "CLUSTER_FAILURE_REPORT role=worker action=summary_qualified reporter=" << watcherAddr_
                  << " target=" << targetAddress << " failed_count=" << failedCount << " failed_ms=" << failedMs;
        WakeKeepAliveForFailureSummary();
    } else {
        VLOG(1) << "CLUSTER_FAILURE_OBSERVE role=worker reporter=" << watcherAddr_ << " target=" << targetAddress
                << " failed_count=" << failedCount << " failed_ms=" << failedMs;
    }
}

void DsCoordinationBackend::RecordPeerRpcSuccess(const HostPort &target)
{
    ClearPeerRpcFailure(target, "success_reset");
}

bool DsCoordinationBackend::IsPeerRpcFailureReported(const HostPort &target) const
{
    std::lock_guard<std::mutex> lock(rpcFailedMutex_);
    const auto state = rpcFailedStates_.find(target.ToString());
    return state != rpcFailedStates_.end() && state->second.reported;
}

void DsCoordinationBackend::DiscardPeerRpcFailure(const HostPort &target)
{
    ClearPeerRpcFailure(target, "incarnation_reset");
}

void DsCoordinationBackend::ClearPeerRpcFailure(const HostPort &target, const char *action)
{
    const auto targetAddress = target.ToString();
    if (targetAddress.empty()) {
        return;
    }
    if (!hasRpcFailures_.load(std::memory_order_acquire)) {
        return;
    }
    bool shouldWake = false;
    {
        std::lock_guard<std::mutex> lock(rpcFailedMutex_);
        const auto found = rpcFailedStates_.find(targetAddress);
        const bool existed = found != rpcFailedStates_.end();
        const bool reported = existed && found->second.reported;
        if (existed) {
            rpcFailedStates_.erase(found);
        }
        if (reported) {
            immediateReportSignal_ = true;
            shouldWake = true;
            LOG(INFO) << "CLUSTER_FAILURE_OBSERVE role=worker reporter=" << watcherAddr_ << " target=" << targetAddress
                      << " action=" << action;
        } else if (existed) {
            VLOG(1) << "CLUSTER_FAILURE_OBSERVE role=worker reporter=" << watcherAddr_ << " target=" << targetAddress
                    << " action=" << action;
        }
        if (rpcFailedStates_.empty()) {
            if (!shouldWake) {
                immediateReportSignal_ = false;
            }
            hasRpcFailures_.store(false, std::memory_order_release);
        }
    }
    if (shouldWake) {
        WakeKeepAliveForFailureSummary();
    }
}

void DsCoordinationBackend::WakeKeepAliveForFailureSummary()
{
    {
        std::lock_guard<std::mutex> lock(keepAliveMutex_);
        ++keepAliveWakeEpoch_;
    }
    keepAliveCv_.notify_all();
}

void DsCoordinationBackend::ClearPeerRpcFailureObservations()
{
    std::lock_guard<std::mutex> lock(rpcFailedMutex_);
    rpcFailedStates_.clear();
    immediateReportSignal_ = false;
    hasRpcFailures_.store(false, std::memory_order_release);
}

std::vector<std::string> DsCoordinationBackend::GetFailedTargets(std::chrono::steady_clock::time_point now)
{
    const auto failureWindow =
        std::chrono::milliseconds(static_cast<int64_t>(FLAGS_node_timeout_s) * MS_PER_SECOND
                                  / PEER_RPC_FAILURE_WINDOW_DIVISOR);
    const auto activeWindow = std::chrono::milliseconds(static_cast<int64_t>(FLAGS_node_timeout_s) * MS_PER_SECOND * 2);
    if (!hasRpcFailures_.load(std::memory_order_acquire)) {
        return {};
    }
    std::vector<std::string> targets;
    {
        std::lock_guard<std::mutex> lock(rpcFailedMutex_);
        for (auto iter = rpcFailedStates_.begin(); iter != rpcFailedStates_.end();) {
            const auto &state = iter->second;
            if (now - state.lastFailedAt > activeWindow) {
                iter = rpcFailedStates_.erase(iter);
                continue;
            }
            if (state.failedCount >= MIN_PEER_RPC_FAILURES_TO_REPORT && now - state.firstFailedAt >= failureWindow) {
                targets.push_back(iter->first);
            }
            ++iter;
        }
        if (rpcFailedStates_.empty()) {
            immediateReportSignal_ = false;
            hasRpcFailures_.store(false, std::memory_order_release);
        }
    }
    std::sort(targets.begin(), targets.end());
    if (!targets.empty()) {
        VLOG(1) << "CLUSTER_FAILURE_REPORT role=worker reporter=" << watcherAddr_
                << " targets=" << VectorToString(targets);
    }
    return targets;
}

bool DsCoordinationBackend::ConsumeImmediateReportSignal()
{
    std::lock_guard<std::mutex> lock(rpcFailedMutex_);
    const bool signal = immediateReportSignal_;
    immediateReportSignal_ = false;
    return signal;
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
    int64_t intervalMs = keepAliveIntervalMs_;
    constexpr int64_t maxIntervalMs = 5'000;
    constexpr int64_t minIntervalMs = 100;
    intervalMs = std::max(minIntervalMs, std::min(intervalMs, maxIntervalMs));
    KeepAliveFailureState state;
    INJECT_POINT("CoordinationBackend.KeepAlive.intervalMs", [&intervalMs](int timeMs) { intervalMs = timeMs; });
    INJECT_POINT("CoordinationBackend.KeepAlive.confirmTimes", [&state](int times) { state.confirmMinTimes = times; });
    const std::string realKey = BuildRealKey(keepAliveTableName_, keepAliveKey_);
    while (!keepAliveExit_) {
        uint64_t wakeEpoch = 0;
        {
            std::lock_guard<std::mutex> lock(keepAliveMutex_);
            wakeEpoch = keepAliveWakeEpoch_;
        }
        auto rc = RenewKeepAliveOnce();
        VLOG(1) << "Member " << watcherAddr_ << " keepalive result: " << rc.ToString();
        if (rc.IsOk()) {
            HandleKeepAliveSuccess(state);
        } else {
            HandleKeepAliveFailure(rc, realKey, state);
        }
        std::unique_lock<std::mutex> lock(keepAliveMutex_);
        keepAliveCv_.wait_for(lock, std::chrono::milliseconds(intervalMs), [this, wakeEpoch]() {
            if (keepAliveExit_.load()) {
                return true;
            }
            if (keepAliveWakeEpoch_ != wakeEpoch) {
                static_cast<void>(ConsumeImmediateReportSignal());
                return true;
            }
            const bool immediateReport = ConsumeImmediateReportSignal();
            INJECT_POINT_NO_RETURN("CoordinationBackend.KeepAlive.afterWakePredicate");
            return immediateReport;
        });
    }
}

void DsCoordinationBackend::HandleKeepAliveSuccess(KeepAliveFailureState &state)
{
    state.confirmTimes = 0;
    state.needHandleFailure = true;
}

void DsCoordinationBackend::HandleMembershipSuccess(const std::string &coordinatorId, uint64_t successEpoch,
                                                    bool recreated)
{
    MembershipReadyHandler handler;
    bool identityChanged = false;
    if (!PrepareMembershipSuccess(coordinatorId, successEpoch, handler, identityChanged)) {
        return;
    }
    const bool invalidated = InvalidateMembershipWatches(coordinatorId, successEpoch, recreated);
    if (invalidated) {
        DispatchWatchEvent({ CoordinationEventType::RESET, "", "", 0, 0 });
    }
    if (identityChanged || recreated) {
        LOG(INFO) << "CLUSTER_COORDINATOR_ID role=worker watcher=" << watcherAddr_
                  << ", id=" << BytesUuidToString(coordinatorId).substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE)
                  << ", membership_recreated=" << recreated << ", watches_invalidated=" << invalidated;
    }
    if (handler != nullptr) {
        InvokeMembershipReadyHandler(handler, coordinatorId, successEpoch, recreated || invalidated);
    }
}

bool DsCoordinationBackend::PrepareMembershipSuccess(const std::string &coordinatorId, uint64_t successEpoch,
                                                     MembershipReadyHandler &handler, bool &identityChanged)
{
    std::lock_guard<std::mutex> lock(eventHandlerMutex_);
    if (successEpoch <= lastMembershipSuccessEpoch_) {
        return false;
    }
    lastMembershipSuccessEpoch_ = successEpoch;
    identityChanged = !coordinatorId.empty() && lastMembershipCoordinatorId_ != coordinatorId;
    lastMembershipCoordinatorId_ = coordinatorId;
    handler = membershipReadyHandler_;
    if (handler != nullptr) {
        ++activeMembershipReadyHandlers_;
    }
    return true;
}

bool DsCoordinationBackend::InvalidateMembershipWatches(const std::string &coordinatorId, uint64_t successEpoch,
                                                        bool recreated)
{
    std::lock_guard<std::mutex> lock(watchMutex_);
    bool invalidated = false;
    if (successEpoch >= lastWatchMembershipSuccessEpoch_ && !watchPlan_.empty() && !rewatchRequired_
        && (recreated || registeredCoordinatorId_ != coordinatorId)) {
        rewatchRequired_ = true;
        invalidated = true;
    }
    lastWatchMembershipSuccessEpoch_ = std::max(lastWatchMembershipSuccessEpoch_, successEpoch);
    return invalidated;
}

void DsCoordinationBackend::InvokeMembershipReadyHandler(const MembershipReadyHandler &handler,
                                                         const std::string &coordinatorId, uint64_t successEpoch,
                                                         bool refreshRequired)
{
    bool current = false;
    {
        std::lock_guard<std::mutex> lock(eventHandlerMutex_);
        current = successEpoch == lastMembershipSuccessEpoch_;
    }
    try {
        if (current) {
            handler(coordinatorId, refreshRequired);
        }
    } catch (const std::exception &error) {
        LOG(ERROR) << "Coordinator membership-ready handler threw: " << error.what();
    } catch (...) {
        LOG(ERROR) << "Coordinator membership-ready handler threw an unknown exception";
    }
    std::lock_guard<std::mutex> lock(eventHandlerMutex_);
    --activeMembershipReadyHandlers_;
    eventHandlerCv_.notify_all();
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
    const bool storeAvailable = CheckStoreAvailableAfterKeepAliveFailure(state);
    if (storeAvailable && ++state.confirmTimes >= state.confirmMinTimes) {
        HandleKeepAliveFailed(realKey);
        state.needHandleFailure = false;
        LOG(WARNING) << "Confirmed local Coordinator network isolation; keep the process alive and report the "
                        "membership deletion event.";
    } else if (status.GetCode() == K_NOT_FOUND || status.GetCode() == K_TRY_AGAIN) {
        MembershipReconcileHandler reconcile;
        {
            std::lock_guard<std::mutex> lock(eventHandlerMutex_);
            reconcile = membershipReconcileHandler_;
        }
        if (reconcile != nullptr) {
            LOG_IF_ERROR(reconcile(false), "CLUSTER_MEMBERSHIP_RECONCILE_SCHEDULE_FAILED");
        } else {
            (void)AutoCreateKeepAliveKey(true);
        }
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
    LOG_IF_ERROR(proxy_->CancelWatch(watcherAddr_, watchIds, watchCoordinatorId),
                 "Cancel coordinator watches failed");
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
        membershipReadyHandler_ = {};
        membershipReconcileHandler_ = {};
    }
    {
        std::lock_guard<std::mutex> rewatchLock(rewatchMutex_);
        std::lock_guard<std::mutex> lock(watchMutex_);
        watchStopping_ = true;
    }
    ShutdownKeepAliveThread();
    CancelWatches();
    std::unique_lock<std::mutex> lock(eventHandlerMutex_);
    eventHandlerCv_.wait(lock, [this] {
        return activeEventHandlers_ == 0 && activeMembershipReadyHandlers_ == 0;
    });
    return Status::OK();
}

Status DsCoordinationBackend::Shutdown()
{
    return ShutdownEventSources();
}

Status DsCoordinationBackend::UpdateNodeState(MemberLifecycleState state)
{
    return UpdateNodeStateWithTimeout(state, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS);
}

Status DsCoordinationBackend::UpdateNodeStateWithTimeout(MemberLifecycleState state, int32_t timeoutMs)
{
    CHECK_FAIL_RETURN_STATUS(proxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator service proxy is null");
    if (state == MemberLifecycleState::EXITING) {
        exitMembershipRequested_.store(true, std::memory_order_release);
    }
    CHECK_FAIL_RETURN_STATUS(timeoutMs > 0, K_RPC_DEADLINE_EXCEEDED,
                             "Membership lifecycle update timeout expired");
    const auto effectiveTimeoutMs = std::min(timeoutMs, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(effectiveTimeoutMs);
    auto pendingState = state;
    for (size_t attempt = 0; attempt < MAX_MEMBERSHIP_MUTATION_ATTEMPTS; ++attempt) {
        bool retry = false;
        auto status = PublishNodeStateMutation(pendingState, deadline, retry);
        if (status.IsError() || !retry) {
            return status;
        }
    }
    RETURN_STATUS(K_TRY_AGAIN, "membership changed repeatedly while lifecycle update was in flight");
}

Status DsCoordinationBackend::PublishNodeStateMutation(MemberLifecycleState &state,
                                                       std::chrono::steady_clock::time_point deadline, bool &retry)
{
    const auto remainingMs = coordination_backend_detail::RemainingTimeoutMs(
        deadline, std::chrono::steady_clock::now());
    CHECK_FAIL_RETURN_STATUS(remainingMs > 0, K_RPC_DEADLINE_EXCEEDED,
                             "Membership lifecycle update timeout expired");
    MembershipMutation mutation;
    RETURN_IF_NOT_OK(PrepareNodeStateMutation(state, remainingMs, mutation));
    RETURN_OK_IF_TRUE(!mutation.required);
    const auto rpcTimeoutMs = coordination_backend_detail::RemainingTimeoutMs(
        deadline, std::chrono::steady_clock::now());
    CHECK_FAIL_RETURN_STATUS(rpcTimeoutMs > 0, K_RPC_DEADLINE_EXCEEDED,
                             "Membership lifecycle update timeout expired");
    int64_t version = 0;
    int64_t revision = 0;
    std::string coordinatorId;
    RETURN_IF_NOT_OK(proxy_->Put(BuildRealKey(keepAliveTableName_, keepAliveKey_), mutation.encodedValue,
                                 keepAliveTtlMs_,
                                 COORDINATOR_NO_VERSION_CHECK, version, revision,
                                 rpcTimeoutMs, &coordinatorId,
                                 mutation.startedFrom.coordinatorId, mutation.startedFrom.modRevision));
    RETURN_IF_NOT_OK(CommitNodeStateMutation(mutation, coordinatorId, revision, retry));
    if (retry && exitMembershipRequested_.load(std::memory_order_acquire)) {
        state = MemberLifecycleState::EXITING;
    }
    return Status::OK();
}

Status DsCoordinationBackend::PrepareNodeStateMutation(MemberLifecycleState state, int32_t timeoutMs,
                                                       MembershipMutation &mutation)
{
    const auto operation = state == MemberLifecycleState::EXITING
                               ? MembershipMutationOperation::MARK_EXITING
                               : MembershipMutationOperation::UPDATE_MEMBERSHIP_STATE;
    const auto waitStartedAt = std::chrono::steady_clock::now();
    timespec deadlineTs = butil::milliseconds_from_now(timeoutMs);
    if (!membershipMutationMutex_.timed_lock(&deadlineTs)) {
        RETURN_STATUS(K_RPC_DEADLINE_EXCEEDED,
                      "Membership lifecycle update timed out waiting for serialization; "
                          + GetMembershipMutationDiagnostic(operation, waitStartedAt));
    }
    {
        MembershipMutationGuard mutationGuard(*this, operation, std::adopt_lock);
        mutationGuard.SetPhase(MembershipMutationPhase::PREPARE_PAYLOAD);
        if (state == MemberLifecycleState::EXITING) {
            std::lock_guard<std::mutex> lock(keepAliveMutex_);
            keepAliveValue_.lifecycleState = MemberLifecycleState::EXITING;
        } else if (exitMembershipRequested_.load(std::memory_order_acquire)) {
            mutation.required = false;
            return Status::OK();
        }
        CHECK_FAIL_RETURN_STATUS(!IsKeepAliveTimeout(), K_NOT_READY,
                                 "The key written to the cluster table must be bound to a lease");
        mutation.startedFrom = GetMembershipIncarnationLocked();
        std::lock_guard<std::mutex> lock(keepAliveMutex_);
        mutation.value = keepAliveValue_;
        mutation.value.lifecycleState = state;
    }
    return MembershipValueCodec::Encode(mutation.value, mutation.encodedValue);
}

Status DsCoordinationBackend::CommitNodeStateMutation(const MembershipMutation &mutation,
                                                      const std::string &coordinatorId, int64_t revision, bool &retry)
{
    MembershipCommitResult result;
    uint64_t successEpoch = 0;
    retry = false;
    {
        const auto operation = mutation.value.lifecycleState == MemberLifecycleState::EXITING
                                   ? MembershipMutationOperation::MARK_EXITING
                                   : MembershipMutationOperation::UPDATE_MEMBERSHIP_STATE;
        MembershipMutationGuard mutationGuard(*this, operation);
        mutationGuard.SetPhase(MembershipMutationPhase::UPDATE_LOCAL_MEMBERSHIP);
        result = CommitMembershipRevisionLocked(mutation.startedFrom, coordinatorId, revision);
        if (result == MembershipCommitResult::COMMITTED) {
            std::lock_guard<std::mutex> lock(keepAliveMutex_);
            const bool sameProcess = keepAliveValue_.timestamp == mutation.value.timestamp;
            const bool allowedByExit = !exitMembershipRequested_.load(std::memory_order_acquire)
                                       || mutation.value.lifecycleState == MemberLifecycleState::EXITING;
            if (sameProcess && allowedByExit) {
                keepAliveValue_ = mutation.value;
                successEpoch = ++membershipSuccessEpoch_;
            } else {
                retry = true;
            }
        } else if (result == MembershipCommitResult::SUPERSEDED) {
            retry = true;
        }
    }
    CHECK_FAIL_RETURN_STATUS(result != MembershipCommitResult::STALE_LIFETIME, K_TRY_AGAIN,
                             "membership Coordinator changed while lifecycle update was in flight");
    if (result == MembershipCommitResult::COMMITTED && successEpoch > 0) {
        HandleMembershipSuccess(coordinatorId, successEpoch);
    }
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
    RETURN_OK_IF_TRUE(exitMembershipRequested_.load(std::memory_order_acquire));
    const auto workerAddress = workerAddr.ToString();
    const bool updatesLocalMembership = workerAddress == keepAliveKey_;
    MembershipMutation mutation;
    if (updatesLocalMembership) {
        MembershipMutationGuard mutationGuard(*this, MembershipMutationOperation::INFORM_RECONCILIATION_DONE);
        mutationGuard.SetPhase(MembershipMutationPhase::PREPARE_PAYLOAD);
        mutation.startedFrom = GetMembershipIncarnationLocked();
    }
    const std::string realKey = BuildRealKey(keepAliveTableName_, workerAddress);
    std::vector<KeyValueEntry> entries;
    int64_t revision = 0;
    std::string rangeCoordinatorId;
    auto rangeStatus = proxy_->Range(realKey, "", entries, revision, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS,
                                     &rangeCoordinatorId);
    RefreshWatchIdentity(rangeStatus);
    RETURN_IF_NOT_OK(rangeStatus);
    CHECK_FAIL_RETURN_STATUS(!entries.empty(), K_NOT_FOUND, "membership does not exist during reconciliation");
    MembershipValue value;
    RETURN_IF_NOT_OK(MembershipValueCodec::Decode(entries.front().value, value));
    if (value.lifecycleState == MemberLifecycleState::RESTARTING
        || value.lifecycleState == MemberLifecycleState::RECOVERING) {
        RETURN_OK_IF_TRUE(exitMembershipRequested_.load(std::memory_order_acquire));
        value.lifecycleState = MemberLifecycleState::READY;
        std::string readyValue;
        RETURN_IF_NOT_OK(MembershipValueCodec::Encode(value, readyValue));
        int64_t version = 0;
        int64_t putRevision = 0;
        std::string coordinatorId;
        auto putStatus = proxy_->Put(realKey, readyValue, keepAliveTtlMs_, entries.front().version, version,
                                     putRevision, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS, &coordinatorId,
                                     rangeCoordinatorId, entries.front().modRevision);
        RefreshWatchIdentity(putStatus);
        RETURN_IF_NOT_OK(putStatus);
        if (updatesLocalMembership) {
            mutation.value = value;
            bool retry = false;
            RETURN_IF_NOT_OK(CommitNodeStateMutation(mutation, coordinatorId, putRevision, retry));
            const auto retryState = exitMembershipRequested_.load(std::memory_order_acquire)
                                        ? MemberLifecycleState::EXITING
                                        : MemberLifecycleState::READY;
            return retry ? UpdateNodeState(retryState) : Status::OK();
        }
        HandleRemoteMembershipSuccess(coordinatorId);
    }
    return Status::OK();
}

void DsCoordinationBackend::HandleRemoteMembershipSuccess(const std::string &coordinatorId)
{
    uint64_t successEpoch = 0;
    {
        MembershipMutationGuard mutationGuard(*this, MembershipMutationOperation::INFORM_RECONCILIATION_DONE);
        mutationGuard.SetPhase(MembershipMutationPhase::UPDATE_LOCAL_MEMBERSHIP);
        successEpoch = ++membershipSuccessEpoch_;
    }
    HandleMembershipSuccess(coordinatorId, successEpoch);
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

void DsCoordinationBackend::SetCheckStoreStateWhenNetworkFailedHandler(std::function<bool()> handler)
{
    std::lock_guard<std::mutex> lock(eventHandlerMutex_);
    checkStoreStateWhenNetworkFailedHandler_ = std::move(handler);
}

const std::string &DsCoordinationBackend::GetWatcherAddr() const
{
    return watcherAddr_;
}

void DsCoordinationBackend::SetMembershipReadyHandler(MembershipReadyHandler handler)
{
    std::string coordinatorId;
    {
        std::lock_guard<std::mutex> lock(eventHandlerMutex_);
        membershipReadyHandler_ = handler;
        coordinatorId = lastMembershipCoordinatorId_;
    }
    if (handler != nullptr && !coordinatorId.empty()) {
        handler(coordinatorId, false);
    }
}

Status DsCoordinationBackend::GetMembershipRenewalPayload(MembershipRenewalPayload &payload) const
{
    payload = MembershipRenewalPayload{};
    MembershipValue value;
    {
        std::lock_guard<std::mutex> lock(keepAliveMutex_);
        CHECK_FAIL_RETURN_STATUS(!keepAliveKey_.empty() && keepAliveTtlMs_ > 0
                                     && keepAliveValue_.lifecycleState != MemberLifecycleState::UNKNOWN,
                                 K_NOT_READY, "membership lease is not initialized");
        value = keepAliveValue_;
        payload.reporterAddress = keepAliveKey_;
        payload.ttlMs = keepAliveTtlMs_;
    }
    if (exitMembershipRequested_.load(std::memory_order_acquire)) {
        value.lifecycleState = MemberLifecycleState::EXITING;
    }
    return MembershipValueCodec::Encode(value, payload.encodedValue);
}

void DsCoordinationBackend::SetMembershipReconcileHandler(MembershipReconcileHandler handler)
{
    std::lock_guard<std::mutex> lock(eventHandlerMutex_);
    membershipReconcileHandler_ = std::move(handler);
}

void DsCoordinationBackend::SetMembershipRecreateGate(MembershipRecreateGate gate)
{
    std::lock_guard<std::mutex> lock(eventHandlerMutex_);
    membershipRecreateGate_ = std::move(gate);
}

Status DsCoordinationBackend::PrepareMembershipRecreate()
{
    MembershipRecreateGate recreateGate;
    {
        std::lock_guard<std::mutex> lock(eventHandlerMutex_);
        recreateGate = membershipRecreateGate_;
    }
    // Run the rejoin cleanup gate outside eventHandlerMutex_; the callback closes local admissions and may enter
    // worker cleanup/RPC paths before the new keepalive revision is installed.
    if (recreateGate != nullptr) {
        RETURN_IF_NOT_OK(recreateGate());
    }
    return Status::OK();
}

void DsCoordinationBackend::InstallEnsuredMembership(const std::string &coordinatorId, int64_t membershipModRevision)
{
    MembershipIncarnation startedFrom;
    {
        MembershipMutationGuard mutationGuard(*this, MembershipMutationOperation::INSTALL_ENSURED_MEMBERSHIP);
        mutationGuard.SetPhase(MembershipMutationPhase::PREPARE_PAYLOAD);
        startedFrom = GetMembershipIncarnationLocked();
    }
    static_cast<void>(InstallEnsuredMembership(startedFrom, coordinatorId, membershipModRevision));
}

DsCoordinationBackend::MembershipIncarnation DsCoordinationBackend::GetMembershipIncarnationLocked() const
{
    return { membershipCoordinatorId_, keepAliveModRevision_ };
}

bool DsCoordinationBackend::IsMembershipIncarnationCurrentLocked(const MembershipIncarnation &incarnation) const
{
    return membershipCoordinatorId_ == incarnation.coordinatorId
           && keepAliveModRevision_ == incarnation.modRevision;
}

DsCoordinationBackend::MembershipCommitResult DsCoordinationBackend::CommitMembershipRevisionLocked(
    const MembershipIncarnation &startedFrom, const std::string &coordinatorId, int64_t revision)
{
    if (membershipCoordinatorId_ == coordinatorId) {
        if (keepAliveModRevision_ > revision) {
            return MembershipCommitResult::SUPERSEDED;
        }
    } else if (!IsMembershipIncarnationCurrentLocked(startedFrom)) {
        return MembershipCommitResult::STALE_LIFETIME;
    }
    membershipCoordinatorId_ = coordinatorId;
    keepAliveModRevision_ = revision;
    return MembershipCommitResult::COMMITTED;
}

DsCoordinationBackend::MembershipCommitResult DsCoordinationBackend::InstallEnsuredMembership(
    const MembershipIncarnation &startedFrom, const std::string &coordinatorId, int64_t membershipModRevision)
{
    MembershipCommitResult result;
    uint64_t successEpoch = 0;
    {
        MembershipMutationGuard mutationGuard(*this, MembershipMutationOperation::INSTALL_ENSURED_MEMBERSHIP);
        mutationGuard.SetPhase(MembershipMutationPhase::INSTALL_REVISION);
        result = CommitMembershipRevisionLocked(startedFrom, coordinatorId, membershipModRevision);
        if (result == MembershipCommitResult::COMMITTED) {
            successEpoch = ++membershipSuccessEpoch_;
            keepAliveTimeout_ = false;
            firstKeepAliveSent_.store(true, std::memory_order_release);
            std::lock_guard<std::mutex> lock(keepAliveMutex_);
            if (exitMembershipRequested_.load(std::memory_order_acquire)) {
                keepAliveValue_.lifecycleState = MemberLifecycleState::EXITING;
            } else if (keepAliveValue_.lifecycleState == MemberLifecycleState::STARTING
                       || keepAliveValue_.lifecycleState == MemberLifecycleState::RESTARTING) {
                keepAliveValue_.lifecycleState = MemberLifecycleState::RECOVERING;
            }
            ++keepAliveWakeEpoch_;
        }
    }
    if (result == MembershipCommitResult::COMMITTED) {
        HandleMembershipSuccess(coordinatorId, successEpoch, true);
    }
    if (result == MembershipCommitResult::COMMITTED) {
        keepAliveCv_.notify_all();
    }
    return result;
}

Status DsCoordinationBackend::RepairEnsuredMembership(const MembershipValue &ensuredValue,
                                                      MembershipCommitResult result)
{
    if (result != MembershipCommitResult::COMMITTED) {
        return Status::OK();
    }
    MembershipValue currentValue;
    {
        std::lock_guard<std::mutex> lock(keepAliveMutex_);
        currentValue = keepAliveValue_;
    }
    const bool sameProcess = currentValue.timestamp == ensuredValue.timestamp;
    const bool needsRepair = currentValue.lifecycleState == MemberLifecycleState::READY
                             || currentValue.lifecycleState == MemberLifecycleState::EXITING;
    if (!sameProcess || !needsRepair || currentValue.lifecycleState == ensuredValue.lifecycleState) {
        return Status::OK();
    }
    return UpdateNodeState(currentValue.lifecycleState);
}

Status DsCoordinationBackend::EnsureMembership(const std::string &coordinatorId,
                                               const MembershipEnsureHandler &ensure,
                                               bool markRestarting)
{
    CHECK_FAIL_RETURN_STATUS(ensure != nullptr, K_INVALID, "Membership Ensure handler is empty");
    MembershipRenewalPayload payload;
    MembershipIncarnation startedFrom;
    {
        MembershipMutationGuard mutationGuard(*this, MembershipMutationOperation::ENSURE_MEMBERSHIP);
        mutationGuard.SetPhase(MembershipMutationPhase::PREPARE_PAYLOAD);
        startedFrom = GetMembershipIncarnationLocked();
        if (markRestarting && !exitMembershipRequested_.load(std::memory_order_acquire)) {
            std::lock_guard<std::mutex> lock(keepAliveMutex_);
            keepAliveValue_.lifecycleState = MemberLifecycleState::RESTARTING;
            keepAliveValue_.timestamp = std::chrono::system_clock::now().time_since_epoch().count();
        }
        RETURN_IF_NOT_OK(GetMembershipRenewalPayload(payload));
    }
    MembershipValue ensuredValue;
    RETURN_IF_NOT_OK(MembershipValueCodec::Decode(payload.encodedValue, ensuredValue));
    int64_t membershipModRevision = 0;
    RETURN_IF_NOT_OK(ensure(payload, membershipModRevision));
    CHECK_FAIL_RETURN_STATUS(membershipModRevision > 0, K_TRY_AGAIN,
                             "Coordinator accepted membership Ensure without a revision");
    const auto result = InstallEnsuredMembership(startedFrom, coordinatorId, membershipModRevision);
    CHECK_FAIL_RETURN_STATUS(result != MembershipCommitResult::STALE_LIFETIME, K_TRY_AGAIN,
                             "Coordinator changed while membership Ensure was in flight");
    return RepairEnsuredMembership(ensuredValue, result);
}

Status DsCoordinationBackend::OnMembershipEnsured(const std::string &coordinatorId, int64_t membershipModRevision)
{
    RETURN_IF_NOT_OK(PrepareMembershipRecreate());
    CHECK_FAIL_RETURN_STATUS(membershipModRevision > 0, K_TRY_AGAIN,
                             "Coordinator accepted membership Ensure without a revision");
    MembershipIncarnation startedFrom;
    {
        MembershipMutationGuard mutationGuard(*this, MembershipMutationOperation::INSTALL_ENSURED_MEMBERSHIP);
        mutationGuard.SetPhase(MembershipMutationPhase::PREPARE_PAYLOAD);
        startedFrom = GetMembershipIncarnationLocked();
    }
    const auto result = InstallEnsuredMembership(startedFrom, coordinatorId, membershipModRevision);
    CHECK_FAIL_RETURN_STATUS(result != MembershipCommitResult::STALE_LIFETIME, K_TRY_AGAIN,
                             "Coordinator changed while membership Ensure was being installed");
    if (result == MembershipCommitResult::COMMITTED
        && exitMembershipRequested_.load(std::memory_order_acquire)) {
        return UpdateNodeState(MemberLifecycleState::EXITING);
    }
    return Status::OK();
}

bool DsCoordinationBackend::OwnsWatchIdentity(const std::string &coordinatorId, int64_t watchId) const
{
    std::lock_guard<std::mutex> lock(watchMutex_);
    return OwnsWatchIdentityLocked(coordinatorId, watchId);
}

Status DsCoordinationBackend::CommitIfCurrentWatch(const std::string &coordinatorId, int64_t watchId,
                                                   const std::function<Status()> &commit)
{
    CHECK_FAIL_RETURN_STATUS(commit != nullptr, K_INVALID, "current watch commit is empty");
    std::lock_guard<std::mutex> lock(watchMutex_);
    CHECK_FAIL_RETURN_STATUS(OwnsWatchIdentityLocked(coordinatorId, watchId),
                             K_NOT_READY, "Coordinator watch registration is no longer authoritative");
    return commit();
}

bool DsCoordinationBackend::OwnsWatchIdentityLocked(const std::string &coordinatorId, int64_t watchId) const
{
    return !watchStopping_ && !rewatchRequired_ && registeredCoordinatorId_ == coordinatorId
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
    event.sourceAuthorityId = coordinatorId;
    event.sourceWatchId = watchId;
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
