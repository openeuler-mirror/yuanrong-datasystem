/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Approved internal access to opaque storage scan segments.
 */
#ifndef DATASYSTEM_CLUSTER_EXECUTOR_STORAGE_SCAN_PLAN_ACCESS_H
#define DATASYSTEM_CLUSTER_EXECUTOR_STORAGE_SCAN_PLAN_ACCESS_H

#include <cstdint>
#include <vector>

#include "datasystem/cluster/executor/storage_scan_plan.h"
#include "datasystem/utils/status.h"

namespace datasystem::cluster {

/**
 * @brief One topology-internal closed logical scan segment.
 */
struct StorageScanSegment {
    uint32_t from{ 0 };
    uint32_t end{ 0 };
};

/**
 * @brief Internal access gate for approved storage bridges.
 */
class StorageScanPlanAccess final {
public:
    /**
     * @brief Copy validated segments.
     * @param[in] plan Opaque plan.
     * @param[out] segments Canonical segments.
     * @return Operation status.
     */
    static Status GetSegments(const StorageScanPlan &plan, std::vector<StorageScanSegment> &segments);

private:
    /**
     * @brief Prohibit construction of the static access gate.
     */
    StorageScanPlanAccess() = delete;

    /**
     * @brief Prohibit destruction of the static access gate.
     */
    ~StorageScanPlanAccess() = delete;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_EXECUTOR_STORAGE_SCAN_PLAN_ACCESS_H
