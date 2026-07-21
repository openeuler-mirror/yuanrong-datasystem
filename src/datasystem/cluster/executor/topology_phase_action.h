/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Topology task participant facts passed to business callbacks.
 */
#ifndef DATASYSTEM_CLUSTER_EXECUTOR_TOPOLOGY_PHASE_ACTION_H
#define DATASYSTEM_CLUSTER_EXECUTOR_TOPOLOGY_PHASE_ACTION_H

#include <cstdint>
#include <optional>
#include <string>

#include "datasystem/cluster/model/topology_types.h"

namespace datasystem::cluster {

/**
 * @brief Algorithm-neutral callback participant facts.
 */
struct TopologyPhaseAction {
    std::string taskId;
    uint64_t topologyVersion{ 0 };
    uint64_t batchEpoch{ 0 };
    MemberIdentity executor;
    std::optional<MemberIdentity> source;
    std::optional<MemberIdentity> target;
    std::optional<MemberIdentity> failed;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_EXECUTOR_TOPOLOGY_PHASE_ACTION_H
