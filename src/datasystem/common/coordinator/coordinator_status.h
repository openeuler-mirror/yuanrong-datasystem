/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_STATUS_H
#define DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_STATUS_H

#include "datasystem/utils/status.h"

namespace datasystem {
/**
 * @brief Classify a conflict returned by a Coordinator conditional Put/CAS.
 * K_DATA_INCONSISTENCY: expected key version, modification revision, or global revision no longer matches.
 * K_DUPLICATED: a create-only write found an existing key. K_NOT_FOUND: the fenced key disappeared.
 * Only apply this classification to Coordinator CAS paths; these codes have other meanings elsewhere.
 * A conflict does not refresh local state or authorize replay: callers must reread/reconcile the expected
 * identity and revision, preserve lifecycle fences, and retry within their operation budget.
 * K_TRY_AGAIN (e.g. authority changes) and RPC errors retain their separate caller-specific handling.
 */
inline bool IsCoordinatorCasConflict(const Status &status)
{
    return status.GetCode() == K_DATA_INCONSISTENCY || status.GetCode() == K_DUPLICATED
           || status.GetCode() == K_NOT_FOUND;
}
}  // namespace datasystem
#endif
