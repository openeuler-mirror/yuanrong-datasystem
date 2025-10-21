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
 * Description: Add metrics blocking vector for remote stream worker sending data.
 */

#ifndef DATASYSTEM_COMMON_METRICS_METRICS_SC_REMOTE_VECTOR_H
#define DATASYSTEM_COMMON_METRICS_METRICS_SC_REMOTE_VECTOR_H

#include <mutex>
#include <numeric>
#include <vector>

#include "datasystem/common/metrics/metrics_vector/metrics_blocking_vector.h"

namespace datasystem {
class MetricsScRemoteVector : public MetricsBlockingVector {
public:
    /**
     * @brief Blocking emplace value to vector.
     * @param[in] value The float value.
     */
    void BlockingEmplaceBack(float value) override
    {
        MetricsBlockingVector::BlockingEmplaceBack(value);
    };

    /**
     * @brief Obtain the success rate for each data transmission to the remote endpoint. if FLAGS_log_monitor is true.
     * @param[in] successNum The success num of sending.
     * @param[in] totalNum The total num of sending.
     */
    void BlockingEmplaceBack(int successNum, int totalNum)
    {
        if (totalNum <= 0) {
            MetricsBlockingVector::BlockingEmplaceBack(1.0f);
        } else {
            MetricsBlockingVector::BlockingEmplaceBack(successNum / static_cast<float>(totalNum));
        }
    }
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_METRICS_METRICS_SC_REMOTE_VECTOR_H
