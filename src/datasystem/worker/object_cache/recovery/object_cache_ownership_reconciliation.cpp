/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Object-cache ownership reconciliation planning.
 */
#include "datasystem/worker/object_cache/recovery/object_cache_ownership_reconciliation.h"

namespace datasystem {
namespace object_cache {
namespace {
std::string PendingEvidenceDetail(OwnershipReconciliationKind kind)
{
    switch (kind) {
        case OwnershipReconciliationKind::LOCAL_ISOLATION:
            return "local isolation ownership reconciliation pending";
        case OwnershipReconciliationKind::NETWORK_RECOVERY:
            return "network recovery ownership reconciliation pending";
        case OwnershipReconciliationKind::RESTART:
        default:
            return "restart reconciliation pending";
    }
}

std::string EmptyOwnersMessage(OwnershipReconciliationKind kind)
{
    switch (kind) {
        case OwnershipReconciliationKind::LOCAL_ISOLATION:
            return "No committed metadata owner is available for local-isolation handoff";
        case OwnershipReconciliationKind::NETWORK_RECOVERY:
            return "No committed metadata owner is available for network recovery";
        case OwnershipReconciliationKind::RESTART:
        default:
            return "No committed metadata owner is available for restart reconciliation";
    }
}
}  // namespace

Status BuildOwnershipReconciliationPlan(bool centralizedMetadata, const std::string &localMasterAddress,
                                        const std::string &localWorkerAddress,
                                        const std::vector<std::string> &committedAddresses,
                                        OwnershipReconciliationKind kind, ObjectCacheOwnershipReconciliationPlan &plan)
{
    plan = {};
    plan.pendingEvidenceDetail = PendingEvidenceDetail(kind);
    plan.emptyOwnersMessage = EmptyOwnersMessage(kind);
    if (centralizedMetadata) {
        if (!localMasterAddress.empty()) {
            plan.metadataOwners.emplace(localMasterAddress);
        }
    } else {
        plan.metadataOwners.insert(committedAddresses.begin(), committedAddresses.end());
        if (!localWorkerAddress.empty()) {
            plan.metadataOwners.emplace(localWorkerAddress);
        }
    }
    if (plan.metadataOwners.empty()) {
        return Status(K_NOT_READY, __LINE__, __FILE__, plan.emptyOwnersMessage);
    }
    return Status::OK();
}

}  // namespace object_cache
}  // namespace datasystem
