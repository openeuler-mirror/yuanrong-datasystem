/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef DATASYSTEM_CONTEXT_CONTEXT_H
#define DATASYSTEM_CONTEXT_CONTEXT_H

#include <string>

#include "datasystem/utils/status.h"

namespace datasystem {
class __attribute((visibility("default"))) Context {
public:
    /**
     * @brief Set trace id for all API calls of the current thread.
     * @param[in] traceId The trace id.
     * @return K_OK on success.
     *         K_INVALID: The traceId is invalid.
     */
    static Status SetTraceId(const std::string &traceId);

    /**
     * @brief Set tenantId for all API calls of the current thread. There is no impact in token authentication scenario.
     * @param[in] tenantId The tenant id.
     */
    static void SetTenantId(const std::string &tenantId);
};
}  // namespace datasystem
#endif