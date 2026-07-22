/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Coordinator-backed implementation of the cluster coordination contract.
 */
#ifndef DATASYSTEM_CLUSTER_COORDINATION_BACKEND_DS_COORDINATION_BACKEND_H
#define DATASYSTEM_CLUSTER_COORDINATION_BACKEND_DS_COORDINATION_BACKEND_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "datasystem/cluster/coordination_backend/coordination_backend.h"
#include "datasystem/cluster/membership/membership_value_codec.h"
#include "datasystem/common/coordinator/coordinator_service_proxy.h"
#include "datasystem/common/util/thread.h"

namespace datasystem::cluster {

/**
 * @brief Adapt the DataSystem Coordinator KV/watch/lease APIs to the cluster coordination contract.
 */
class DsCoordinationBackend final : public ICoordinationBackend {
public:
    using MembershipReadyHandler = std::function<void(const std::string &, bool)>;

    /**
     * @brief Construct a Coordinator-backed coordination backend.
     * @param[in] proxy Coordinator proxy that must outlive this backend.
     * @param[in] watcherAddr Canonical Worker address used to register watches.
     */
    DsCoordinationBackend(ICoordinatorServiceProxy *proxy, std::string watcherAddr);

    /**
     * @brief Stop event sources and release Coordinator watches.
     */
    ~DsCoordinationBackend() override;

    DsCoordinationBackend(const DsCoordinationBackend &) = delete;
    DsCoordinationBackend &operator=(const DsCoordinationBackend &) = delete;

    /**
     * @brief Read every key/value pair below a logical table.
     * @param[in] tableName Logical table name.
     * @param[out] outKeyValues Relative keys and values returned by Coordinator.
     * @return Status of the call.
     */
    Status GetAll(const std::string &tableName,
                  std::vector<std::pair<std::string, std::string>> &outKeyValues) override;

    /**
     * @brief Read one value from a logical table.
     * @param[in] tableName Logical table name.
     * @param[in] key Relative key within the table.
     * @param[out] value Stored value.
     * @return Status of the call.
     */
    Status Get(const std::string &tableName, const std::string &key, std::string &value) override;

    /**
     * @brief Read one value together with its Coordinator version metadata.
     * @param[in] tableName Logical table name.
     * @param[in] key Relative key within the table.
     * @param[out] res Stored value and version metadata.
     * @param[in] timeoutMs RPC timeout in milliseconds.
     * @return Status of the call.
     */
    Status Get(const std::string &tableName, const std::string &key, RangeSearchResult &res,
               int32_t timeoutMs = SEND_RPC_TIMEOUT_MS_DEFAULT) override;

    /**
     * @brief Register one logical table prefix.
     * @param[in] tableName Logical table name.
     * @param[in] tablePrefix Physical table prefix.
     * @return Status of the call.
     */
    Status CreateTable(const std::string &tableName, const std::string &tablePrefix) override;

    /**
     * @brief Register one already-canonical logical table prefix.
     * @param[in] tableName Logical table name.
     * @param[in] tablePrefix Canonical physical table prefix.
     * @return Status of the call.
     */
    Status CreateTableWithExactPrefix(const std::string &tableName, const std::string &tablePrefix) override;

    /**
     * @brief Put one value into a logical table.
     * @param[in] tableName Logical table name.
     * @param[in] key Relative key within the table.
     * @param[in] value Stored value.
     * @return Status of the call.
     */
    Status Put(const std::string &tableName, const std::string &key, const std::string &value) override;

    /**
     * @brief Atomically transform one value and return its committed metadata.
     * @param[in] tableName Logical table name.
     * @param[in] key Relative key within the table.
     * @param[in] processFunc Deterministic compare-and-transform callback.
     * @param[out] res Committed value and version metadata.
     * @return Status of the call.
     */
    Status CAS(const std::string &tableName, const std::string &key, const ProcessFunction &processFunc,
               RangeSearchResult &res) override;

    /**
     * @brief Atomically transform one value.
     * @param[in] tableName Logical table name.
     * @param[in] key Relative key within the table.
     * @param[in] processFunc Deterministic compare-and-transform callback.
     * @return Status of the call.
     */
    Status CAS(const std::string &tableName, const std::string &key, const ProcessFunction &processFunc) override;

    /**
     * @brief Replace one value only when the observed bytes still match.
     * @param[in] tableName Logical table name.
     * @param[in] key Relative key within the table.
     * @param[in] oldValue Expected stored bytes.
     * @param[in] newValue Replacement bytes.
     * @return Status of the call.
     */
    Status CAS(const std::string &tableName, const std::string &key, const std::string &oldValue,
               const std::string &newValue) override;

    /**
     * @brief Delete one key with the default timeout.
     * @param[in] tableName Logical table name.
     * @param[in] key Relative key within the table.
     * @return Status of the call.
     */
    Status Delete(const std::string &tableName, const std::string &key) override;

    /**
     * @brief Delete one key with a caller-supplied timeout.
     * @param[in] tableName Logical table name.
     * @param[in] key Relative key within the table.
     * @param[in] timeoutMs RPC timeout in milliseconds.
     * @return Status of the call.
     */
    Status Delete(const std::string &tableName, const std::string &key, int timeoutMs) override;

    /**
     * @brief Register Coordinator watches and deliver their initial snapshots.
     * @param[in] watchKeys Exact or prefix watch descriptors.
     * @return Status of the call.
     */
    Status WatchEvents(const std::vector<WatchKey> &watchKeys) override;

    /**
     * @brief Publish the membership lease and start periodic renewal.
     * @param[in] tableName Logical membership table.
     * @param[in] key Canonical member address.
     * @param[in] isRestart Whether this process is restarting an admitted member.
     * @param[in] isStoreAvailableWhenStart Whether Coordinator was reachable at startup.
     * @return Status of the call.
     */
    Status InitKeepAlive(const std::string &tableName, const std::string &key, bool isRestart,
                         bool isStoreAvailableWhenStart) override;

    /**
     * @brief Stop lease renewal and watches before their consumers are destroyed.
     * @return Status of the call.
     */
    Status ShutdownEventSources() override;

    /**
     * @brief Stop watches without touching membership keepalive.
     * @return Status of the call.
     */
    Status ShutdownWatchEventSources() override;

    /**
     * @brief Shut down this backend idempotently.
     * @return Status of the call.
     */
    Status Shutdown() override;

    /**
     * @brief Publish a new membership lifecycle state under the current lease.
     * @param[in] state New lifecycle state.
     * @return Status of the call.
     */
    Status UpdateNodeState(MemberLifecycleState state) override;

    /**
     * @brief Resolve one logical table to the physical Coordinator prefix.
     * @param[in] tableName Logical table name.
     * @param[out] prefix Physical Coordinator prefix.
     * @return Status of the call.
     */
    Status GetStorePrefix(const std::string &tableName, std::string &prefix) override;

    /**
     * @brief Promote a restarting or recovering member to READY after reconciliation.
     * @param[in] workerAddr Canonical member address.
     * @return Status of the call.
     */
    Status InformReconciliationDone(const HostPort &workerAddr) override;

    /**
     * @brief Report whether the current membership renewal is timed out.
     * @return True when the latest renewal failed.
     */
    bool IsKeepAliveTimeout() override;

    /**
     * @brief Report whether the initial membership lease has been published.
     * @return True after the initial lease is published, independent of later lifecycle transitions.
     */
    bool IsFirstKeepAliveSent() override;

    /**
     * @brief Install the single event consumer for this backend instance.
     * @param[in] eventHandler Event callback, or an empty callback to detach.
     */
    void SetEventHandler(EventHandler &&eventHandler) override;

    /**
     * @brief Install the Coordinator member local-isolation callback.
     * @param[in] handler Callback to consume.
     */
    void SetLocalIsolationHandler(LocalIsolationHandler handler) override;

    /**
     * @brief Install the Coordinator member local-recovery callback.
     * @param[in] handler Callback to consume.
     */
    void SetLocalRecoveryHandler(LocalRecoveryHandler handler) override;

    /**
     * @brief Install the local evidence callback used to classify Coordinator renewal failures.
     * @param[in] handler Callback returning whether the backing store remains reachable from peers.
     */
    void SetCheckStoreStateWhenNetworkFailedHandler(std::function<bool()> handler) override;

    /**
     * @brief Return the Worker address registered for Coordinator watches.
     * @return Stable watcher address.
     */
    const std::string &GetWatcherAddr() const;

    /**
     * @brief Install a callback for exact membership-operation CoordinatorIds.
     * @param[in] handler Callback invoked outside backend locks with identity and watch-invalidation state.
     */
    void SetMembershipReadyHandler(const MembershipReadyHandler &handler) override;

    /**
     * @brief Check whether this backend owns one CoordinatorId/watch identity.
     * @param[in] coordinatorId Coordinator process-lifetime identity.
     * @param[in] watchId Watch identity within that Coordinator lifetime.
     * @return True only for a committed current registration.
     */
    bool OwnsWatchIdentity(const std::string &coordinatorId, int64_t watchId) const override;

    /**
     * @brief Check whether a watch registration transaction is in progress.
     * @return True while an RPC may have created a channel not yet known by watch ID.
     */
    bool IsWatchRegistrationInProgress() const override;

    /**
     * @brief Mark the saved watch plan stale and enqueue a non-blocking exact-read doorbell.
     */
    void InvalidateWatches() override;

    /**
     * @brief Deliver one identity-bound Coordinator watch event.
     * @param[in] coordinatorId Coordinator process-lifetime identity.
     * @param[in] watchId Watch identity within that Coordinator lifetime.
     * @param[in] event Event delivered by the Worker watch RPC service.
     */
    void HandleWatchEvent(const std::string &coordinatorId, int64_t watchId, CoordinationEvent &&event) override;

private:
    struct KeepAliveFailureState;
    struct WatchedKey {
        std::string key;
        bool isPrefix{ false };
    };
    struct WatchRegistration {
        int64_t watchId{ 0 };
        WatchedKey scope;
    };

    /**
     * @brief Strip a physical table prefix from one returned key.
     * @param[in] key Physical Coordinator key.
     * @param[in] prefix Physical table prefix.
     * @return Relative logical key, or the input key when it is outside the prefix.
     */
    static std::string RemoveTablePrefix(const std::string &key, const std::string &prefix);

    /**
     * @brief Build one physical Coordinator key.
     * @param[in] tableName Logical table name.
     * @param[in] key Relative key within the table.
     * @return Physical Coordinator key.
     */
    std::string BuildRealKey(const std::string &tableName, const std::string &key);

    /**
     * @brief Create or recreate the leased membership key.
     * @param[in] recreated True when existing watch ownership must be invalidated.
     * @return Status of the call.
     */
    Status AutoCreateKeepAliveKey(bool recreated = false);

    /**
     * @brief Renew the current membership lease once.
     * @return Status of the call.
     */
    Status RenewKeepAliveOnce();

    /**
     * @brief Start the named membership renewal thread.
     */
    void LaunchKeepAliveThread();

    /**
     * @brief Run periodic lease renewal until shutdown.
     */
    void RunKeepAliveLoop();

    /**
     * @brief Reset renewal-failure evidence after a successful keepalive.
     * @param[in,out] state Failure evidence accumulated by the renewal loop.
     */
    void HandleKeepAliveSuccess(KeepAliveFailureState &state);

    /**
     * @brief Record one successful membership CoordinatorId and request rewatch when it changes.
     * @param[in] coordinatorId Exact successful membership response identity.
     * @param[in] recreated True when the membership key was newly created or recreated.
     */
    void HandleMembershipSuccess(const std::string &coordinatorId, bool recreated = false);

    /**
     * @brief Classify and handle one failed lease renewal.
     * @param[in] status Renewal status.
     * @param[in] realKey Physical membership key.
     * @param[in,out] state Failure evidence accumulated by the renewal loop.
     */
    void HandleKeepAliveFailure(const Status &status, const std::string &realKey, KeepAliveFailureState &state);

    /**
     * @brief Query peer/store evidence after a renewal failure.
     * @param[in,out] state Failure evidence reset when the store is unavailable to peers.
     * @return True when peer evidence says the backing store remains available.
     */
    bool CheckStoreAvailableAfterKeepAliveFailure(KeepAliveFailureState &state);

    /**
     * @brief Synthesize a membership DELETE event after confirmed lease loss.
     * @param[in] realKey Physical membership key.
     */
    void HandleKeepAliveFailed(const std::string &realKey);

    /**
     * @brief Cancel every Coordinator watch owned by this backend instance.
     */
    void CancelWatches();

    /**
     * @brief Stop and join the membership renewal thread.
     */
    void ShutdownKeepAliveThread();

    /**
     * @brief Register and atomically commit one complete logical watch plan.
     * @param[in] watchKeys Logical watch plan.
     * @return K_OK after complete registration or an RPC/identity status without partial commit.
     */
    Status RegisterWatchPlan(const std::vector<WatchKey> &watchKeys);

    /**
     * @brief Build all physical registrations and initial snapshot events for a logical plan.
     * @param[in] watchKeys Logical watch plan.
     * @param[out] registrations Successful physical registrations.
     * @param[out] registeredIds Successful watch IDs for rollback.
     * @param[out] initialEvents Initial snapshot events.
     * @param[out] coordinatorId Coordinator lifetime shared by the complete batch.
     * @return K_OK for a complete same-lifetime batch, otherwise an RPC or identity status.
     */
    Status PrepareWatchPlan(const std::vector<WatchKey> &watchKeys, std::vector<WatchRegistration> &registrations,
                            std::vector<int64_t> &registeredIds, std::vector<CoordinationEvent> &initialEvents,
                            std::string &coordinatorId);

    /**
     * @brief Atomically replace the committed registrations after a complete batch succeeds.
     * @param[in] watchKeys Logical watch plan.
     * @param[in] registrations Complete physical registrations.
     * @param[in] coordinatorId Coordinator lifetime that owns registrations.
     */
    void CommitWatchPlan(const std::vector<WatchKey> &watchKeys, std::vector<WatchRegistration> registrations,
                         const std::string &coordinatorId);

    /**
     * @brief Re-register the saved watch plan after a CoordinatorId change or RESET.
     * @return K_OK when no rewatch is needed or after a complete replacement.
     */
    Status RewatchIfNeeded();

    /**
     * @brief Observe successful identity or probe after a cold identity-ambiguous failure.
     * @param[in] status Completed backend RPC status.
     */
    void RefreshWatchIdentity(const Status &status);

    /**
     * @brief Deliver one already fenced watch doorbell to the installed consumer.
     * @param[in] event Move-only event.
     */
    void DispatchWatchEvent(CoordinationEvent &&event);

    /**
     * @brief Check whether one physical event key belongs to a registered watch.
     * @param[in] watchId Coordinator watch identity.
     * @param[in] key Physical Coordinator event key.
     * @return True when this backend instance subscribed to the key.
     */
    bool AcceptsWatchEvent(int64_t watchId, const std::string &key) const;

    ICoordinatorServiceProxy *proxy_;
    std::string watcherAddr_;
    mutable std::mutex tableMutex_;
    std::unordered_map<std::string, std::string> tableMap_;
    std::string pendingWatchRegistrationId_;
    // Serializes complete watch registration transactions without being held by callbacks or RPC helpers.
    std::mutex rewatchMutex_;
    // Protects watch state, identity probe backoff and nextIdentityProbeAt_.
    mutable std::mutex watchMutex_;
    std::vector<WatchKey> watchPlan_;
    std::vector<WatchRegistration> registrations_;
    std::string registeredCoordinatorId_;
    bool watchRegistrationInProgress_{ false };
    bool rewatchRequired_{ false };
    bool watchStopping_{ false };
    static constexpr std::chrono::milliseconds INITIAL_IDENTITY_PROBE_BACKOFF{ 100 };
    static constexpr std::chrono::milliseconds MAX_IDENTITY_PROBE_BACKOFF{ 5'000 };
    static constexpr int IDENTITY_PROBE_BACKOFF_MULTIPLIER = 2;
    std::chrono::milliseconds identityProbeBackoff_{ INITIAL_IDENTITY_PROBE_BACKOFF };
    std::chrono::steady_clock::time_point nextIdentityProbeAt_;

    // Protects event handlers, store-state callback, last membership identity and activeEventHandlers_.
    std::mutex eventHandlerMutex_;
    // Uses eventHandlerMutex_ to drain handler copies before their consumer is destroyed.
    std::condition_variable eventHandlerCv_;
    EventHandler eventHandler_;
    MembershipReadyHandler membershipReadyHandler_;
    std::string lastMembershipCoordinatorId_;
    LocalIsolationHandler localIsolationHandler_;
    LocalRecoveryHandler localRecoveryHandler_;
    std::function<bool()> checkStoreStateWhenNetworkFailedHandler_;
    size_t activeEventHandlers_{ 0 };

    std::string keepAliveTableName_;
    std::string keepAliveKey_;
    // Serializes complete membership state mutations and their RPC commit order.
    std::mutex membershipMutationMutex_;
    // Protects keepAliveValue_; also used by keepAliveCv_ to interrupt its wait.
    std::mutex keepAliveMutex_;
    MembershipValue keepAliveValue_;
    std::condition_variable keepAliveCv_;
    Thread keepAliveThread_;
    std::atomic<bool> keepAliveExit_{ false };
    std::atomic<bool> keepAliveTimeout_{ false };
    // Monotonic publication fact; unlike keepAliveValue_, it does not regress when lifecycle advances to READY.
    std::atomic<bool> firstKeepAliveSent_{ false };
    static constexpr int64_t MS_PER_SECOND = 1'000;
    int64_t keepAliveTtlMs_{ 0 };
};

}  // namespace datasystem::cluster
#endif  // DATASYSTEM_CLUSTER_COORDINATION_BACKEND_DS_COORDINATION_BACKEND_H
