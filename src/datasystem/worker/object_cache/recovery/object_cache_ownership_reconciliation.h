/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Object-cache ownership reconciliation planning.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_RECOVERY_OBJECT_CACHE_OWNERSHIP_RECONCILIATION_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_RECOVERY_OBJECT_CACHE_OWNERSHIP_RECONCILIATION_H

#include <set>
#include <string>
#include <vector>

#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {

enum class OwnershipReconciliationKind {
    RESTART,
    LOCAL_ISOLATION,
    NETWORK_RECOVERY,
};

struct ObjectCacheOwnershipReconciliationPlan {
    std::set<std::string> metadataOwners;
    std::string pendingEvidenceDetail;
    std::string emptyOwnersMessage;
};

struct ObjectCacheOwnershipReconciliationRequest {
    bool centralizedMetadata = false;
    std::string localMasterAddress;
    std::string localWorkerAddress;
    std::vector<std::string> committedAddresses;
    OwnershipReconciliationKind kind = OwnershipReconciliationKind::RESTART;
};

Status BuildOwnershipReconciliationPlan(const ObjectCacheOwnershipReconciliationRequest &request,
                                        ObjectCacheOwnershipReconciliationPlan &plan);

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_RECOVERY_OBJECT_CACHE_OWNERSHIP_RECONCILIATION_H
