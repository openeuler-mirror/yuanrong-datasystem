/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: ETCD-only stale cluster topology task cleanup.
 */
#ifndef DATASYSTEM_CLUSTER_CONTROL_TOPOLOGY_TASK_JANITOR_H
#define DATASYSTEM_CLUSTER_CONTROL_TOPOLOGY_TASK_JANITOR_H

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>

#include "datasystem/cluster/control/topology_task_materializer.h"
#include "datasystem/cluster/repository/topology_repository.h"
#include "datasystem/common/util/thread.h"

namespace datasystem::cluster {

/**
 * @brief Bounded cleanup cadence and per-pass work limits.
 */
struct TopologyTaskJanitorOptions {
    std::chrono::seconds interval{ 60 };
    size_t scanLimit{ 8'192 };
    size_t deleteBatch{ 128 };
};

/**
 * @brief Remove stale ETCD task records without participating in topology correctness.
 */
class TopologyTaskJanitor final {
public:
    /**
     * @brief Bind non-owned cleanup dependencies.
     * @param[in] repository Repository that outlives this Janitor.
     * @param[in] algorithm Planning algorithm that outlives this Janitor.
     * @param[in] materializer Materializer that outlives this Janitor.
     * @param[in] options Bounded cleanup options.
     */
    TopologyTaskJanitor(TopologyRepository &repository, const IPlanningAlgorithm &algorithm,
                        TopologyTaskMaterializer &materializer, TopologyTaskJanitorOptions options);

    /**
     * @brief Stop and join the cleanup thread before releasing Janitor state.
     */
    ~TopologyTaskJanitor();

    /**
     * @brief Disable copying a cleanup-thread owner.
     */
    TopologyTaskJanitor(const TopologyTaskJanitor &) = delete;

    /**
     * @brief Disable copy assignment of a cleanup-thread owner.
     */
    TopologyTaskJanitor &operator=(const TopologyTaskJanitor &) = delete;

    /**
     * @brief Start periodic cleanup.
     * @return Validation or thread-start status.
     */
    Status Start();

    /**
     * @brief Stop and join by an absolute deadline.
     * @param[in] deadline Absolute steady-clock deadline.
     * @return K_OK after join or K_RPC_DEADLINE_EXCEEDED while dependencies remain required.
     */
    Status Stop(std::chrono::steady_clock::time_point deadline);

    /**
     * @brief Run one serialized bounded pass.
     * @return Read, derivation, or backend status.
     */
    Status RunOnce();

private:
    /**
     * @brief Run immediate and periodic cleanup until Stop is requested.
     */
    void Run();

    TopologyRepository &repository_;
    const IPlanningAlgorithm &algorithm_;
    TopologyTaskMaterializer &materializer_;
    TopologyTaskJanitorOptions options_;
    // Protects thread_, started_, stopping_, and threadExited_ during lifecycle transitions.
    std::mutex lifecycleMutex_;
    // Uses lifecycleMutex_ to wake the janitor when stopping_ changes or the next pass is due.
    std::condition_variable wakeCv_;
    // Uses lifecycleMutex_ to signal changes to threadExited_.
    std::condition_variable stoppedCv_;
    // Serializes RunOnce() cleanup passes; it does not protect lifecycle member variables.
    std::mutex passMutex_;
    Thread thread_;
    bool started_{ false };
    bool stopping_{ false };
    bool threadExited_{ true };
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_CONTROL_TOPOLOGY_TASK_JANITOR_H
