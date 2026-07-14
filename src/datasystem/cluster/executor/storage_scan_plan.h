/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Opaque read-only storage candidate scan plan.
 */
#ifndef DATASYSTEM_CLUSTER_EXECUTOR_STORAGE_SCAN_PLAN_H
#define DATASYSTEM_CLUSTER_EXECUTOR_STORAGE_SCAN_PLAN_H

#include <memory>
#include <string>
#include <vector>

#include "datasystem/cluster/model/topology_types.h"

namespace datasystem::cluster {

class StorageScanPlanAccess;
class TopologyTaskExecutor;

/**
 * @brief Opaque candidate scan plan owned for one synchronous callback invocation.
 */
class StorageScanPlan final {
public:
    /**
     * @brief Destroy the opaque scan-plan handle.
     */
    ~StorageScanPlan() = default;

    /**
     * @brief Disable copying an invocation-scoped plan.
     */
    StorageScanPlan(const StorageScanPlan &) = delete;

    /**
     * @brief Disable copy assignment of an invocation-scoped plan.
     */
    StorageScanPlan &operator=(const StorageScanPlan &) = delete;

    /**
     * @brief Disable moving an Executor-owned plan.
     */
    StorageScanPlan(StorageScanPlan &&) = delete;

    /**
     * @brief Disable move assignment of an Executor-owned plan.
     */
    StorageScanPlan &operator=(StorageScanPlan &&) = delete;

    /**
     * @brief Return bounded diagnostics without internal segments.
     * @return Payload-free text.
     */
    std::string DebugString() const;

private:
    friend class StorageScanPlanAccess;
    friend class TopologyTaskExecutor;
    struct Impl;

    /**
     * @brief Construct from an immutable implementation.
     * @param[in] impl Internal implementation.
     */
    explicit StorageScanPlan(std::shared_ptr<const Impl> impl);

    /**
     * @brief Build a hash candidate plan.
     * @param[in] ranges Canonical task ranges.
     * @return Owned opaque plan.
     */
    static std::unique_ptr<StorageScanPlan> CreateHash(const std::vector<TokenRange> &ranges);
    std::shared_ptr<const Impl> impl_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_EXECUTOR_STORAGE_SCAN_PLAN_H
