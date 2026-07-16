/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Business-neutral cluster placement results.
 */
#ifndef DATASYSTEM_CLUSTER_ROUTING_PLACEMENT_TYPES_H
#define DATASYSTEM_CLUSTER_ROUTING_PLACEMENT_TYPES_H

#include <string>
#include <vector>

#include "datasystem/cluster/model/topology_types.h"
#include "datasystem/utils/status.h"

namespace datasystem::cluster {

struct PlacementDecision {
    uint64_t topologyVersion{ 0 };
    std::string committedOwnerAddress;
};

/**
 * @brief One per-key placement result within a single-Snapshot batch.
 */
struct BatchPlacementItem {
    PlacementDecision decision;
    Status status;
};

struct BatchPlacementDecision {
    uint64_t topologyVersion{ 0 };
    std::vector<BatchPlacementItem> items;
};
enum class RedirectAction : uint8_t { LOCAL, REDIRECT, WAIT };
struct RedirectDecision {
    uint64_t topologyVersion{ 0 };
    RedirectAction action{ RedirectAction::LOCAL };
    std::string committedOwnerAddress;
    // Empty when the committed owner is also the redirect target, avoiding a steady-state address copy.
    std::string redirectTargetAddress;

    /**
     * @brief Return the explicit handoff target or the committed owner when no override exists.
     * @return Redirect target address owned by this decision.
     */
    const std::string &GetRedirectTargetAddress() const noexcept
    {
        return redirectTargetAddress.empty() ? committedOwnerAddress : redirectTargetAddress;
    }
};
struct BatchRedirectDecision {
    uint64_t topologyVersion{ 0 };
    std::vector<RedirectDecision> decisions;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_ROUTING_PLACEMENT_TYPES_H
