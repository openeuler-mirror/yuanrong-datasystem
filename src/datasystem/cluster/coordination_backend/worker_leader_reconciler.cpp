/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
#include "datasystem/cluster/coordination_backend/worker_leader_reconciler.h"

#include <algorithm>
#include <chrono>
#include <functional>
#include <string_view>
#include <thread>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {
namespace {
constexpr auto ENSURE_MIN_RETRY_BACKOFF = std::chrono::milliseconds(50);
constexpr auto ENSURE_MAX_RETRY_BACKOFF = std::chrono::milliseconds(2'000);
constexpr std::chrono::milliseconds::rep ENSURE_RETRY_BACKOFF_MULTIPLIER = 2;
constexpr size_t ENSURE_POOL_SIZE = 1;
constexpr size_t SYNC_ENSURE_MAX_ATTEMPTS = 3;

bool IsRetryableEnsureStatus(const Status &status)
{
    return status.GetCode() == K_TRY_AGAIN || status.GetCode() == K_NOT_READY || status.GetCode() == K_RPC_UNAVAILABLE
           || status.GetCode() == K_RPC_DEADLINE_EXCEEDED;
}

std::chrono::milliseconds EnsureRetryBackoff(const CoordinatorLeaderIdentity &identity, std::string_view workerAddress,
                                             size_t retryAttempt)
{
    auto backoff = ENSURE_MIN_RETRY_BACKOFF;
    for (size_t attempt = 0; attempt < retryAttempt && backoff < ENSURE_MAX_RETRY_BACKOFF; ++attempt) {
        backoff = std::min(ENSURE_MAX_RETRY_BACKOFF, backoff * ENSURE_RETRY_BACKOFF_MULTIPLIER);
    }
    const auto entropy = std::hash<std::string_view>{}(workerAddress) ^ std::hash<std::string>{}(identity.coordinatorId)
                         ^ std::hash<uint64_t>{}(identity.routeEpoch + retryAttempt);
    const auto jitterRange = backoff.count() / 2 + 1;
    const auto jitter = static_cast<int64_t>(entropy % jitterRange) - backoff.count() / 4;
    return std::chrono::milliseconds(backoff.count() + jitter);
}
}  // namespace

WorkerLeaderReconciler::WorkerLeaderReconciler(ICoordinatorServiceProxy &proxy, DsCoordinationBackend &backend,
                                               TopologyRecoveryReporter &reporter, std::string clusterName)
    : proxy_(proxy),
      backend_(backend),
      reporter_(reporter),
      clusterName_(std::move(clusterName)),
      ensurePool_(std::make_unique<ThreadPool>(ENSURE_POOL_SIZE, ENSURE_POOL_SIZE, "WorkerLeaderEnsure", true))
{
    if (auto *routes = proxy_.GetLeaderRouteProvider(); routes != nullptr) {
        subscription_ = routes->SubscribeLeaderChanges(
            [this](const CoordinatorLeaderIdentity &identity) { OnLeaderChanged(identity); });
        OnLeaderChanged(routes->GetLeaderCache());
    }
}

WorkerLeaderReconciler::~WorkerLeaderReconciler()
{
    Shutdown();
}

bool WorkerLeaderReconciler::SameIdentity(const CoordinatorLeaderIdentity &left, const CoordinatorLeaderIdentity &right)
{
    return left.hasLeader == right.hasLeader && left.address.ToString() == right.address.ToString()
           && left.coordinatorId == right.coordinatorId && left.leaderTerm == right.leaderTerm
           && left.routeEpoch == right.routeEpoch;
}

bool WorkerLeaderReconciler::IsCurrentIdentityLocked(const CoordinatorLeaderIdentity &identity) const
{
    return SameIdentity(identity, pendingIdentity_);
}

void WorkerLeaderReconciler::OnLeaderChanged(const CoordinatorLeaderIdentity &identity)
{
    if (stopping_.load(std::memory_order_acquire) || !identity.hasLeader || identity.coordinatorId.empty()) {
        return;
    }
    // InitKeepAlive owns the first normal membership Put. It synchronously calls Reconcile(true) only when
    // that Put is rejected by a recovering Leader, so a route observation cannot race the initial publication.
    if (!backend_.IsFirstKeepAliveSent()) {
        return;
    }
    ScheduleEnsure(identity, false);
}

void WorkerLeaderReconciler::ScheduleEnsure(const CoordinatorLeaderIdentity &identity, bool forceEnsure)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_.load(std::memory_order_relaxed)
        || (!forceEnsure && SameIdentity(identity, lastEnsuredIdentity_))) {
        return;
    }
    pendingIdentity_ = identity;
    forceEnsurePending_ = forceEnsurePending_ || forceEnsure;
    retryCv_.notify_all();
    if (ensureScheduled_ || ensurePool_ == nullptr) {
        return;
    }
    ensureScheduled_ = true;
    try {
        ensurePool_->Execute([this, identity] { RunEnsureLoop(identity); });
    } catch (const std::exception &error) {
        ensureScheduled_ = false;
        LOG(WARNING) << "WORKER_LEADER_ENSURE_SCHEDULE_FAILED cluster=" << clusterName_ << " error=" << error.what();
    }
}

void WorkerLeaderReconciler::NotifyMembershipReady(const std::string &coordinatorId)
{
    if (stopping_.load(std::memory_order_acquire) || coordinatorId.empty()) {
        return;
    }
    const auto *routes = proxy_.GetLeaderRouteProvider();
    if (routes == nullptr) {
        return;
    }
    const auto identity = routes->GetLeaderCache();
    if (!identity.hasLeader || identity.coordinatorId.empty()) {
        return;
    }
    if (identity.coordinatorId == coordinatorId) {
        reporter_.NotifyMembershipReady(identity);
        return;
    }
    ScheduleEnsure(identity, false);
}

Status WorkerLeaderReconciler::Reconcile(bool waitForCompletion)
{
    CHECK_FAIL_RETURN_STATUS(!stopping_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "Worker Leader reconciler is shutting down");
    auto *routes = proxy_.GetLeaderRouteProvider();
    CHECK_FAIL_RETURN_STATUS(routes != nullptr, K_NOT_READY, "Coordinator Leader route is unavailable");
    auto identity = routes->GetLeaderCache();
    CHECK_FAIL_RETURN_STATUS(identity.hasLeader && !identity.coordinatorId.empty(), K_NOT_READY,
                             "Coordinator Leader identity is unavailable");
    if (!waitForCompletion) {
        // Keepalive proved that the membership key or its revision is stale. Queue one more Ensure even when an
        // in-flight request later publishes the same Leader identity.
        ScheduleEnsure(identity, true);
        return Status::OK();
    }
    Status lastStatus(K_TRY_AGAIN, "Coordinator Leader changed during synchronous membership reconciliation");
    for (size_t attempt = 0; attempt < SYNC_ENSURE_MAX_ATTEMPTS; ++attempt) {
        identity = routes->GetLeaderCache();
        CHECK_FAIL_RETURN_STATUS(identity.hasLeader && !identity.coordinatorId.empty(), K_NOT_READY,
                                 "Coordinator Leader identity is unavailable");
        {
            std::lock_guard<std::mutex> lock(mutex_);
            pendingIdentity_ = identity;
        }
        lastStatus = ReconcileIdentity(identity, true);
        if (lastStatus.IsOk() || lastStatus.GetCode() != K_TRY_AGAIN) {
            return lastStatus;
        }
        if (attempt + 1 < SYNC_ENSURE_MAX_ATTEMPTS) {
            std::this_thread::sleep_for(EnsureRetryBackoff(identity, backend_.GetWatcherAddr(), attempt));
        }
    }
    return lastStatus;
}

Status WorkerLeaderReconciler::ReconcileIdentity(const CoordinatorLeaderIdentity &identity, bool forceEnsure)
{
    std::lock_guard<std::mutex> ensureLock(ensureMutex_);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_.load(std::memory_order_acquire)) {
            return Status(K_SHUTTING_DOWN, "Worker Leader reconciler is shutting down");
        }
        if (!forceEnsure && SameIdentity(identity, lastEnsuredIdentity_)) {
            return Status::OK();
        }
    }
    DsCoordinationBackend::MembershipRenewalPayload payload;
    RETURN_IF_NOT_OK(backend_.GetMembershipRenewalPayload(payload));
    coordinator::EnsureLeaderMembershipReqPb request;
    request.set_cluster_name(clusterName_);
    request.set_reporter_address(payload.reporterAddress);
    request.set_coordinator_id(identity.coordinatorId);
    request.set_leader_term(identity.leaderTerm);
    request.set_membership_value(payload.encodedValue);
    request.set_ttl_ms(payload.ttlMs);
    auto *routes = proxy_.GetLeaderRouteProvider();
    CHECK_FAIL_RETURN_STATUS(routes != nullptr, K_NOT_READY, "Coordinator Leader route is unavailable");

    // Rejoin recreates membership in three ordered steps: close local admissions, ask the current Coordinator to
    // install a fresh membership key, then publish the returned revision into the local keepalive state.
    RETURN_IF_NOT_OK(backend_.PrepareMembershipRecreate());
    const auto currentAfterCleanup = routes->GetLeaderCache();
    CHECK_FAIL_RETURN_STATUS(SameIdentity(currentAfterCleanup, identity), K_TRY_AGAIN,
                             "Coordinator Leader changed during membership recreate cleanup");

    coordinator::EnsureLeaderMembershipRspPb response;
    RETURN_IF_NOT_OK(proxy_.EnsureLeaderMembership(request, response));
    const auto currentAfterEnsure = routes->GetLeaderCache();
    CHECK_FAIL_RETURN_STATUS(SameIdentity(currentAfterEnsure, identity), K_TRY_AGAIN,
                             "Coordinator Leader changed during membership ensure");
    CHECK_FAIL_RETURN_STATUS(
        response.result() == coordinator::EnsureLeaderMembershipRspPb::ACCEPTED,
        response.result() == coordinator::EnsureLeaderMembershipRspPb::STALE_EPOCH ? K_TRY_AGAIN : K_INVALID,
        "Coordinator rejected membership ensure");
    CHECK_FAIL_RETURN_STATUS(response.membership_mod_revision() > 0, K_TRY_AGAIN,
                             "Coordinator accepted membership ensure without a revision");
    {
        std::lock_guard<std::mutex> lock(mutex_);
        CHECK_FAIL_RETURN_STATUS(!stopping_.load(std::memory_order_acquire) && IsCurrentIdentityLocked(identity),
                                 K_TRY_AGAIN, "Coordinator Leader changed during membership ensure");
    }
    // Installation synchronously publishes membership readiness. Do not hold mutex_ across that callback: when the
    // Router already observes a successor lifetime, NotifyMembershipReady must be able to queue its fenced Ensure.
    backend_.InstallEnsuredMembership(identity.coordinatorId, response.membership_mod_revision());
    const auto currentAfterInstall = routes->GetLeaderCache();
    CHECK_FAIL_RETURN_STATUS(SameIdentity(currentAfterInstall, identity), K_TRY_AGAIN,
                             "Coordinator Leader changed during membership installation");
    reporter_.NotifyMembershipReady(identity);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!stopping_.load(std::memory_order_acquire) && IsCurrentIdentityLocked(identity)) {
            lastEnsuredIdentity_ = identity;
        }
    }
    return Status::OK();
}

void WorkerLeaderReconciler::RunEnsureLoop(CoordinatorLeaderIdentity identity)
{
    size_t retryAttempt = 0;
    while (!stopping_.load(std::memory_order_acquire)) {
        bool forceEnsure = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopping_.load(std::memory_order_acquire)) {
                ensureScheduled_ = false;
                break;
            }
            identity = pendingIdentity_;
            forceEnsure = forceEnsurePending_;
            forceEnsurePending_ = false;
        }
        const auto status = ReconcileIdentity(identity, forceEnsure);
        if (status.IsOk()) {
            std::unique_lock<std::mutex> lock(mutex_);
            if (IsCurrentIdentityLocked(identity) && !forceEnsurePending_) {
                ensureScheduled_ = false;
                break;
            }
            retryAttempt = 0;
            retryCv_.wait_for(lock, EnsureRetryBackoff(identity, backend_.GetWatcherAddr(), retryAttempt),
                              [this] {
                                  return stopping_.load(std::memory_order_acquire) || forceEnsurePending_;
                              });
            continue;
        }
        std::unique_lock<std::mutex> lock(mutex_);
        if (stopping_.load(std::memory_order_acquire)) {
            ensureScheduled_ = false;
            break;
        }
        if (!IsCurrentIdentityLocked(identity)) {
            retryAttempt = 0;
            retryCv_.wait_for(lock, EnsureRetryBackoff(identity, backend_.GetWatcherAddr(), retryAttempt),
                              [this] { return stopping_.load(std::memory_order_acquire); });
            continue;
        }
        if (forceEnsurePending_) {
            retryAttempt = 0;
            continue;
        }
        if (!IsRetryableEnsureStatus(status)) {
            ensureScheduled_ = false;
            LOG(ERROR) << "WORKER_LEADER_ENSURE_REJECTED cluster=" << clusterName_ << " status=" << status.ToString();
            break;
        }
        retryCv_.wait_for(lock, EnsureRetryBackoff(identity, backend_.GetWatcherAddr(), retryAttempt++),
                          [this, &identity] {
                              return stopping_.load(std::memory_order_acquire) || forceEnsurePending_
                                     || !IsCurrentIdentityLocked(identity);
                          });
    }
    std::lock_guard<std::mutex> lock(mutex_);
    ensureScheduled_ = false;
}

void WorkerLeaderReconciler::Shutdown()
{
    stopping_.store(true, std::memory_order_release);
    retryCv_.notify_all();
    subscription_.reset();
    std::unique_ptr<ThreadPool> ensurePool;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ensurePool = std::move(ensurePool_);
    }
    ensurePool.reset();
}
}  // namespace datasystem::cluster
