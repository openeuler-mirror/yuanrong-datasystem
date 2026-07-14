/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Process-local built-in cluster algorithm catalog.
 */
#include "datasystem/cluster/algorithm/algorithm_catalog.h"

#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {

Status AlgorithmCatalog::GetRouting(const TopologyAlgorithmId &id, const IRoutingAlgorithm *&algorithm) const
{
    CHECK_FAIL_RETURN_STATUS(id == hash_.GetId(), K_NOT_FOUND, "cluster routing algorithm not found");
    algorithm = &hash_;
    return Status::OK();
}

Status AlgorithmCatalog::GetPlanning(const TopologyAlgorithmId &id, const IPlanningAlgorithm *&algorithm) const
{
    CHECK_FAIL_RETURN_STATUS(id == hash_.GetId(), K_NOT_FOUND, "cluster planning algorithm not found");
    algorithm = &hash_;
    return Status::OK();
}

}  // namespace datasystem::cluster
