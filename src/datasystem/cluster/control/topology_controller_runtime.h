/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Single-cluster topology Controller composition and lifecycle.
 */
#ifndef DATASYSTEM_CLUSTER_CONTROL_TOPOLOGY_CONTROLLER_RUNTIME_H
#define DATASYSTEM_CLUSTER_CONTROL_TOPOLOGY_CONTROLLER_RUNTIME_H

#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "datasystem/cluster/control/topology_controller.h"
#include "datasystem/cluster/control/topology_task_janitor.h"
#include "datasystem/cluster/control/topology_task_materializer.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/cluster/repository/topology_repository.h"
#include "datasystem/cluster/runtime/coordination_event_dispatcher.h"

namespace datasystem::cluster {

/**
 * @brief Own all in-process components of one cluster's Controller role.
 *
 * The Runtime borrows one role-exclusive backend and one planning algorithm. It does not own or fully shut down the
 * backend. Start is one-shot, while Stop may be retried after a deadline expires.
 */
class TopologyControllerRuntime final {
public:
    /**
     * @brief Bounded settings for one cluster's Controller composition.
     */
    struct Options {
        std::string clusterName;
        size_t eventQueueCapacity{ 1'024 };
        TopologyControllerOptions controller;
        std::optional<TopologyTaskJanitorOptions> janitor{ std::nullopt };
    };

    /**
     * @brief Validate options and construct one Controller-role composition without runtime side effects.
     * @param[in] options Cluster identity and bounded component settings.
     * @param[in] backend Role-exclusive backend that outlives this Runtime.
     * @param[in] algorithm Planning algorithm that outlives this Runtime.
     * @param[out] runtime Constructed Runtime; unchanged on failure.
     * @return K_OK on success; K_INVALID or construction status otherwise.
     */
    static Status Create(Options options, ICoordinationBackend &backend, const IPlanningAlgorithm &algorithm,
                         std::unique_ptr<TopologyControllerRuntime> &runtime);

    /**
     * @brief Stop and join all owned components before releasing their borrowed dependencies.
     */
    ~TopologyControllerRuntime();

    /**
     * @brief Disable copying a lifecycle and thread composition owner.
     */
    TopologyControllerRuntime(const TopologyControllerRuntime &) = delete;

    /**
     * @brief Disable copy assignment of a lifecycle and thread composition owner.
     */
    TopologyControllerRuntime &operator=(const TopologyControllerRuntime &) = delete;

    /**
     * @brief Start the Controller and optional Janitor exactly once.
     * @return K_OK after both enabled components start; the original startup error otherwise.
     * @note Controller Stop owns controller-role event-source shutdown; this Runtime never fully shuts down the
     *       backend.
     */
    Status Start();

    /**
     * @brief Stop and join all owned components by one absolute deadline.
     * @param[in] deadline Absolute steady-clock deadline passed unchanged to every component.
     * @return K_OK after safe stop; otherwise the first Janitor or Controller error.
     * @note A deadline error retains the Runtime and its borrowed dependencies for a later Stop retry.
     */
    Status Stop(std::chrono::steady_clock::time_point deadline);

    /**
     * @brief Return the Controller's existing no-IO diagnostic snapshot.
     * @return Current Controller diagnostics.
     */
    TopologyControllerDiagnostics GetDiagnostics() const;

private:
    /**
     * @brief Construct prevalidated dependencies in ownership order.
     * @param[in] options Validated options to consume.
     * @param[in] backend Non-owned role backend.
     * @param[in] algorithm Non-owned planning algorithm.
     * @param[in] keys Validated key helper to own.
     */
    TopologyControllerRuntime(Options options, ICoordinationBackend &backend, const IPlanningAlgorithm &algorithm,
                              std::unique_ptr<TopologyKeyHelper> keys);

    /**
     * @brief Claim the one-shot Start transaction without running component code under the lifecycle lock.
     * @return K_OK when claimed; K_TRY_AGAIN for a concurrent lifecycle call; K_INVALID after any Start attempt.
     */
    Status ClaimStart();

    /**
     * @brief Commit whether any component remains live after a claimed Start transaction.
     * @param[in] started True when Stop must still drain at least one started component.
     */
    void CommitStart(bool started);

    /**
     * @brief Claim one Stop transaction without running component code under the lifecycle lock.
     * @param[out] shouldStop True when owned components require a Stop attempt.
     * @return K_OK when claimed or already stopped; K_TRY_AGAIN for a concurrent lifecycle call.
     */
    Status ClaimStop(bool &shouldStop);

    /**
     * @brief Commit one Stop result after all safe component steps were attempted.
     * @param[in] allStopped True when no component retains a live thread.
     */
    void CommitStop(bool allStopped);

    /**
     * @brief Stop every enabled component in reverse startup order.
     * @param[in] deadline Shared absolute deadline for every component.
     * @param[out] allStopped True when every component has joined.
     * @return First component error after all safe Stop steps have been attempted.
     */
    Status StopComponents(std::chrono::steady_clock::time_point deadline, bool &allStopped);

    Options options_;
    ICoordinationBackend &backend_;        // Non-owning; outlives this Runtime.
    const IPlanningAlgorithm &algorithm_;  // Non-owning; outlives this Runtime.
    std::unique_ptr<TopologyKeyHelper> keys_;
    TopologyRepository repository_;
    CoordinationEventDispatcher dispatcher_;
    TopologyTaskMaterializer materializer_;
    TopologyController controller_;
    std::unique_ptr<TopologyTaskJanitor> janitor_;
    // Protects startAttempted_, started_, stopping_, and lifecycleOperationInFlight_. Component Start/Stop and join
    // always execute after releasing this mutex. No component mutex may be held while acquiring lifecycleMutex_.
    std::mutex lifecycleMutex_;
    bool startAttempted_{ false };
    bool started_{ false };
    bool stopping_{ false };
    bool lifecycleOperationInFlight_{ false };
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_CONTROL_TOPOLOGY_CONTROLLER_RUNTIME_H
