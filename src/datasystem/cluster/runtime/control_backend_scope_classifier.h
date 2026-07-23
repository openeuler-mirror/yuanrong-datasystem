/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Control backend failure scope classifier.
 */
#ifndef DATASYSTEM_CLUSTER_CONTROL_BACKEND_SCOPE_CLASSIFIER_H
#define DATASYSTEM_CLUSTER_CONTROL_BACKEND_SCOPE_CLASSIFIER_H

#include <cstdint>
#include <vector>

#include "datasystem/cluster/runtime/control_backend_state.h"

namespace datasystem::cluster {
enum class ControlBackendFailureScope : std::uint8_t {
    INCONCLUSIVE,
    LOCAL_ISOLATION,
    GLOBAL_OUTAGE,
};

ControlBackendFailureScope ClassifyControlBackendFailureScope(
    const ControlBackendObservation &local, const std::vector<MemberIdentity> &targets,
    const std::vector<ControlBackendObservation> &observations);

bool ConfirmsGlobalBackendOutage(const ControlBackendObservation &local, const std::vector<MemberIdentity> &targets,
                                 const std::vector<ControlBackendObservation> &observations);
}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_CONTROL_BACKEND_SCOPE_CLASSIFIER_H
