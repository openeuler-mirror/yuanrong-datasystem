/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Process-local owner of per-cluster topology Controller runtimes.
 */
#ifndef DATASYSTEM_COORDINATOR_TOPOLOGY_CONTROL_HOST_H
#define DATASYSTEM_COORDINATOR_TOPOLOGY_CONTROL_HOST_H

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/control/topology_controller_runtime.h"
#include "datasystem/common/coordinator/watch_event.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/coordinator/coordinator_store_backend.h"
#include "datasystem/coordinator/topology_recovery_manager.h"

namespace datasystem {
class CoordinatorStore;

namespace coordinator {

/**
 * @brief Own one existing TopologyControllerRuntime per admitted cluster.
 */
class TopologyControlHost final {
public:
    struct Options {
        size_t maxClusters{ 8 };
        size_t eventQueueCapacity{ 1'024 };
        std::chrono::milliseconds reconcileInterval{ 100 };
        std::chrono::milliseconds startRetryInitial{ 100 };
        std::chrono::seconds startRetryMaximum{ 5 };
        cluster::TopologyControllerOptions controller;

        /**
         * @brief Validate bounded Host and nested Controller scalar settings.
         * @return True when every setting is usable.
         */
        bool IsValid() const noexcept;
    };

    /**
     * @brief Construct an inert Host with production defaults.
     * @param[in] coordinatorId Immutable process-lifetime CoordinatorId.
     * @param[in] store Process-local Store that outlives the Host.
     * @param[in] recovery Recovery gate that outlives the Host.
     */
    TopologyControlHost(std::string coordinatorId, CoordinatorStore &store, TopologyRecoveryManager &recovery);

    /**
     * @brief Construct an inert Host with explicit bounded options.
     * @param[in] coordinatorId Immutable process-lifetime CoordinatorId.
     * @param[in] store Process-local Store that outlives the Host.
     * @param[in] recovery Recovery gate that outlives the Host.
     * @param[in] options Bounded lifecycle and Controller options.
     */
    TopologyControlHost(std::string coordinatorId, CoordinatorStore &store, TopologyRecoveryManager &recovery,
                        Options options);

    /**
     * @brief Stop all Runtime instances before releasing the Host.
     */
    ~TopologyControlHost();

    /**
     * @brief Disable copying multi-cluster lifecycle ownership.
     */
    TopologyControlHost(const TopologyControlHost &) = delete;

    /**
     * @brief Disable copy assignment of multi-cluster lifecycle ownership.
     */
    TopologyControlHost &operator=(const TopologyControlHost &) = delete;

    /**
     * @brief Start the single Host lifecycle thread.
     * @return K_OK on success or lifecycle/thread status otherwise.
     */
    Status Start();

    /**
     * @brief Reserve cluster capacity before the first membership Store Put.
     * @param[in] clusterName Valid cluster scope; empty is valid.
     * @return K_OK when the cluster is already admitted or a slot is reserved.
     */
    Status PrepareMembershipPut(const std::string &clusterName);

    /**
     * @brief Complete one membership Put reservation.
     * @param[in] clusterName Cluster used by PrepareMembershipPut.
     * @param[in] committed True only after Store Put committed.
     */
    void CompleteMembershipPut(const std::string &clusterName, bool committed) noexcept;

    /**
     * @brief Enqueue a payload-free Store mutation doorbell.
     * @param[in] type Committed mutation type.
     * @param[in] parsed Parsed cluster and key kind.
     */
    void NotifyStoreMutation(WatchEvent::Type type, const ParsedTopologyCoordinationKey &parsed) noexcept;

    /**
     * @brief Stop ingress, stop every Runtime and join the Host thread.
     * @param[in] deadline Absolute steady deadline.
     * @return First stop error after all clusters were attempted.
     */
    Status Shutdown(std::chrono::steady_clock::time_point deadline);

    /**
     * @brief Check whether Host ingress/thread and every owned Runtime are fully stopped.
     * @return True only when borrowed dependencies may be released.
     */
    bool IsStopped() const noexcept;

private:
    enum class EntryState : uint8_t { RESERVED, WAITING_RECOVERY, RUNNING, STOPPING };

    /**
     * @brief Own one cluster's dependencies and Host lifecycle state.
     */
    struct ClusterEntry {
        /**
         * @brief Construct one reserved cluster entry.
         * @param[in] name Valid cluster scope to consume.
         */
        explicit ClusterEntry(std::string name);

        /**
         * @brief Destroy Runtime before its borrowed backend and algorithm.
         */
        ~ClusterEntry();

        ClusterEntry(const ClusterEntry &) = delete;
        ClusterEntry &operator=(const ClusterEntry &) = delete;

        std::string clusterName;
        cluster::HashAlgorithm algorithm;
        std::unique_ptr<CoordinatorStoreBackend> backend;
        std::unique_ptr<cluster::TopologyControllerRuntime> runtime;
        EntryState state{ EntryState::RESERVED };
        size_t pendingMembershipPuts{ 0 };
        bool hasCommittedMembership{ false };
        bool storeDirty{ true };
        bool emptyCheckPending{ false };
        bool releaseAfterStop{ false };
        uint64_t mutationGeneration{ 0 };
        std::chrono::steady_clock::time_point retryAt;
        std::chrono::milliseconds retryBackoff{ 0 };
        std::string stopReason;
    };

    /**
     * @brief Run the single Host lifecycle loop until Shutdown closes ingress.
     */
    void Run() noexcept;

    /**
     * @brief Advance admitted entries without holding the Host lock across component work.
     */
    void ReconcileEntries();

    /**
     * @brief Reconcile one cluster from current Recovery and Store authority.
     * @param[in] clusterName Admitted cluster to reconcile.
     */
    void ReconcileCluster(const std::string &clusterName);

    /**
     * @brief Start a ready entry when its retry deadline has elapsed.
     * @param[in] clusterName Stable entry key.
     * @param[in,out] entry Waiting entry.
     */
    void ReconcileWaitingEntry(const std::string &clusterName, ClusterEntry &entry);

    /**
     * @brief Feed or retire one running Runtime.
     * @param[in] clusterName Stable entry key.
     * @param[in,out] entry Running entry.
     */
    void ReconcileRunningEntry(const std::string &clusterName, ClusterEntry &entry);

    /**
     * @brief Advance one bounded Runtime Stop slice.
     * @param[in] clusterName Stable entry key.
     * @param[in,out] entry Stopping entry.
     */
    void ReconcileStoppingEntry(const std::string &clusterName, ClusterEntry &entry);

    /**
     * @brief Create and one-shot Start a Runtime for one ready entry.
     * @param[in,out] entry Entry that owns the new algorithm, adapter and Runtime.
     * @return Runtime creation or Start status.
     */
    Status StartRuntime(ClusterEntry &entry);

    /**
     * @brief Stop the existing Runtime while retaining it on timeout.
     * @param[in,out] entry Entry whose Runtime is stopping.
     * @param[in] deadline Absolute Stop deadline.
     * @return Runtime Stop status.
     */
    Status StopRuntime(ClusterEntry &entry, std::chrono::steady_clock::time_point deadline);

    /**
     * @brief Check whether membership and topology prove a cluster empty.
     * @param[in,out] entry Candidate entry.
     * @param[out] released True when a stopped entry may be erased.
     * @param[out] observationGeneration Mutation generation captured before Store reads.
     * @return K_OK or the exact-read status that prevented the decision.
     */
    Status ReleaseClusterIfEmpty(ClusterEntry &entry, bool &released, uint64_t &observationGeneration);

    /**
     * @brief Check an empty Store observation against current admission and mutation state.
     * @param[in] entry Entry checked while mutex_ is held.
     * @param[in] observationGeneration Generation captured before the Store reads.
     * @return True only when no concurrent mutation or membership Put invalidated the observation.
     */
    bool IsEmptyObservationCurrent(const ClusterEntry &entry, uint64_t observationGeneration) const noexcept;

    /**
     * @brief Submit one coalesced reset doorbell to a running Runtime.
     * @param[in,out] entry Running entry.
     */
    void SubmitDoorbell(ClusterEntry &entry);

    /**
     * @brief Finish a stopped entry by releasing or scheduling a fresh Runtime.
     * @param[in] clusterName Stable entry key.
     * @param[in,out] entry Stopped entry.
     */
    void FinishStoppedEntry(const std::string &clusterName, ClusterEntry &entry);

    /**
     * @brief Stop all retained Runtime instances during terminal Shutdown.
     * @param[in] deadline Shared absolute deadline.
     * @return First Runtime Stop error after all entries were attempted.
     */
    Status StopAllRuntimes(std::chrono::steady_clock::time_point deadline);

    const std::string coordinatorId_;
    CoordinatorStore &store_;
    TopologyRecoveryManager &recovery_;
    Options options_;

    // Protects lifecycle flags, entries_, counters and mutable admission fields. Runtime/Store calls and thread joins
    // are forbidden while this mutex is held. Runtime ownership is changed only by the Host thread or after it joins.
    mutable std::mutex mutex_;
    std::condition_variable wakeCv_;
    std::unordered_map<std::string, std::unique_ptr<ClusterEntry>> entries_;
    size_t reconcileCursor_{ 0 };
    Thread thread_;
    bool started_{ false };
    bool stopping_{ false };
    bool threadExited_{ true };
    bool threadJoined_{ true };
    bool shutdownInProgress_{ false };
    uint64_t coalescedDoorbells_{ 0 };
    uint64_t runtimeResyncs_{ 0 };
};

}  // namespace coordinator
}  // namespace datasystem

#endif  // DATASYSTEM_COORDINATOR_TOPOLOGY_CONTROL_HOST_H
