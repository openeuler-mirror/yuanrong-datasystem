/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Role-minimal cluster topology watch plans.
 */
#ifndef DATASYSTEM_CLUSTER_RUNTIME_TOPOLOGY_ROLE_WATCH_PLAN_H
#define DATASYSTEM_CLUSTER_RUNTIME_TOPOLOGY_ROLE_WATCH_PLAN_H

#include "datasystem/cluster/coordination_backend/coordination_backend.h"
#include "datasystem/cluster/repository/topology_key_helper.h"

namespace datasystem::cluster {

enum class TopologyRuntimeRole : uint8_t { WORKER, CONTROLLER, OBSERVER };

/**
 * @brief Pure role-to-watch plan.
 */
class TopologyRoleWatchPlan final {
public:
    /**
     * @brief Build the complete role-minimal watch set.
     * @param[in] role Runtime role.
     * @param[in] localAddress Canonical Worker address; empty for other roles.
     * @param[in] keys Cluster-scoped key helper.
     * @param[in] startRevision Non-negative first revision.
     * @param[out] watchKeys Complete descriptors, unchanged on failure.
     * @return K_OK or K_INVALID.
     */
    static Status Build(TopologyRuntimeRole role, const std::string &localAddress, const TopologyKeyHelper &keys,
                        int64_t startRevision, std::vector<WatchKey> &watchKeys);

private:
    TopologyRoleWatchPlan() = delete;
    ~TopologyRoleWatchPlan() = delete;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_RUNTIME_TOPOLOGY_ROLE_WATCH_PLAN_H
