/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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

/**
 * Description: Add metrics blocking vector for obs status code.
 */

#ifndef DATASYSTEM_COMMON_METRICS_METRICS_OBS_CODE_VECTOR_H
#define DATASYSTEM_COMMON_METRICS_METRICS_OBS_CODE_VECTOR_H

#include <mutex>
#include <numeric>
#include <vector>

#include "datasystem/common/metrics/metrics_vector/metrics_blocking_vector.h"

namespace datasystem {
class MetricsObsCodeVector : public MetricsBlockingVector {
public:
    /**
     * @brief Blocking emplace result code to vector.
     * @param[in] code The HTTP status code.
     */
    virtual void BlockingEmplaceBackCode(int code) override
    {
        // HTTP 2xx = success, 404 = not found (treated as success for metrics)
        auto realValue = ((code >= 200 && code < 300) || code == 404) ? 1.0f : 0.0f;
        BlockingEmplaceBack(realValue);
    }
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_METRICS_METRICS_OBS_CODE_VECTOR_H
