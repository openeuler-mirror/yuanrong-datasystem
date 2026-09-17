/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

#ifndef DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_LOG_H
#define DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_LOG_H

#include "datasystem/common/util/uuid_generator.h"

namespace datasystem {
// Log-only prefix: identity comparison and fencing must retain the complete binary UUID.
inline std::string CoordinatorIdLogPrefix(const std::string &coordinatorId)
{
    if (coordinatorId.empty()) {
        return "";
    }
    if (coordinatorId.size() != UUID_SIZE) {
        return "invalid";
    }
    constexpr size_t PREFIX_SIZE = 8;
    return BytesUuidToString(coordinatorId).substr(0, PREFIX_SIZE);
}
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_LOG_H
