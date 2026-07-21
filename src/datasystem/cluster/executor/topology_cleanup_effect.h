/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Bounded topology cleanup effect contract used by business services.
 */
#ifndef DATASYSTEM_CLUSTER_EXECUTOR_TOPOLOGY_CLEANUP_EFFECT_H
#define DATASYSTEM_CLUSTER_EXECUTOR_TOPOLOGY_CLEANUP_EFFECT_H

#include <chrono>
#include <functional>

#include "datasystem/cluster/executor/cancellation_token.h"
#include "datasystem/utils/status.h"

namespace datasystem::cluster {

/**
 * @brief Bounded cleanup effect executed on the callback pool after Snapshot-gated authorization.
 */
using TopologyCleanupEffect =
    std::function<Status(std::chrono::steady_clock::time_point, const CancellationToken &)>;

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_EXECUTOR_TOPOLOGY_CLEANUP_EFFECT_H
