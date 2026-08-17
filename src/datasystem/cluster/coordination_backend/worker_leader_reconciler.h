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
#ifndef DATASYSTEM_CLUSTER_COORDINATION_BACKEND_WORKER_LEADER_RECONCILER_H
#define DATASYSTEM_CLUSTER_COORDINATION_BACKEND_WORKER_LEADER_RECONCILER_H

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>

#include "datasystem/cluster/coordination_backend/ds_coordination_backend.h"
#include "datasystem/cluster/coordination_backend/topology_recovery_reporter.h"
#include "datasystem/common/coordinator/coordinator_leader_router.h"
#include "datasystem/common/coordinator/coordinator_service_proxy.h"
#include "datasystem/common/util/thread_pool.h"

namespace datasystem::cluster {

/**
 * @brief Ensures this Worker's membership for a newly observed Leader before opening the reporter gate.
 */
class WorkerLeaderReconciler final {
public:
    WorkerLeaderReconciler(ICoordinatorServiceProxy &proxy, DsCoordinationBackend &backend,
                           TopologyRecoveryReporter &reporter, std::string clusterName);
    ~WorkerLeaderReconciler();

    // Router callbacks use this non-blocking entry point; Ensure is always done by ensurePool_.
    void OnLeaderChanged(const CoordinatorLeaderIdentity &identity);

    // Complete initial membership publication against the observed Coordinator lifetime.
    void NotifyMembershipReady(const std::string &coordinatorId);

    /**
     * @brief Ensure membership without running local rejoin cleanup.
     * @param[in] waitForCompletion Whether to wait for membership installation.
     * @return K_OK on success; the error code otherwise.
     */
    Status Reconcile(bool waitForCompletion);

    /**
     * @brief Rejoin after the local Worker is confirmed isolated from topology.
     * @return K_OK on success; the error code otherwise.
     */
    Status Rejoin();

    void Shutdown();

private:
    struct EnsureWork {
        CoordinatorLeaderIdentity identity;
        bool forceEnsure{ false };
        bool completeRejoin{ false };
    };

    void ScheduleEnsure(const CoordinatorLeaderIdentity &identity, bool forceEnsure, bool completeRejoin);
    Status ReconcileMembership(bool waitForCompletion, bool completeRejoin);
    Status ReconcileIdentity(const CoordinatorLeaderIdentity &identity, bool forceEnsure, bool completeRejoin);
    Status SendMembershipEnsure(const CoordinatorLeaderIdentity &identity,
                                const DsCoordinationBackend::MembershipRenewalPayload &payload,
                                int64_t &membershipModRevision);
    void RunEnsureLoop(CoordinatorLeaderIdentity identity);
    bool TakePendingEnsure(EnsureWork &work);
    bool FinishSuccessfulEnsure(const EnsureWork &work, size_t &retryAttempt);
    bool FinishFailedEnsure(const EnsureWork &work, const Status &status, size_t &retryAttempt);
    bool IsCurrentIdentityLocked(const CoordinatorLeaderIdentity &identity) const;
    static bool SameIdentity(const CoordinatorLeaderIdentity &left, const CoordinatorLeaderIdentity &right);

    ICoordinatorServiceProxy &proxy_;
    DsCoordinationBackend &backend_;
    TopologyRecoveryReporter &reporter_;
    const std::string clusterName_;
    std::mutex mutex_;
    std::mutex ensureMutex_;
    std::condition_variable retryCv_;
    CoordinatorLeaderIdentity pendingIdentity_;
    CoordinatorLeaderIdentity lastEnsuredIdentity_;
    bool forceEnsurePending_{ false };  // Protected by mutex_; coalesces explicit membership-loss signals.
    bool completeRejoinPending_{ false };
    bool ensureScheduled_{ false };
    std::unique_ptr<ICoordinatorLeaderRouteProvider::Subscription> subscription_;
    std::unique_ptr<ThreadPool> ensurePool_;  // Access and ownership transfer are protected by mutex_.
    std::atomic<bool> stopping_{ false };
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_COORDINATION_BACKEND_WORKER_LEADER_RECONCILER_H
