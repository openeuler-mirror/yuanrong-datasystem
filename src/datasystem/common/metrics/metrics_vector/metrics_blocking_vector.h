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
 * Description: Add metrics blocking vector.
 */

#ifndef DATASYSTEM_COMMON_METRICS_METRICS_BLOCKING_VECTOR_H
#define DATASYSTEM_COMMON_METRICS_METRICS_BLOCKING_VECTOR_H

#include <mutex>
#include <numeric>
#include <vector>

#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/format.h"

namespace datasystem {
class MetricsBlockingVector {
public:
    /**
     * @brief Blocking emplace value to vector.
     * @param[in] value The float value.
     */
    virtual void BlockingEmplaceBack(float value)
    {
        if (FLAGS_log_monitor) {
            std::unique_lock<std::mutex> lock(vecMutex_);
            blockingVec_.emplace_back(value);
        }
    };

    /**
     * @brief Blocking emplace result code to vector.
     * @param[in] code The result code.
     */
    virtual void BlockingEmplaceBackCode(int code)
    {
        auto realValue = (code == 0 ? 1.0f : 0.0f);
        BlockingEmplaceBack(realValue);
    }

    /**
     * @brief Calculate averaged rate of vector.
     * @param[in] defaultValue If vector is empty, return defaultValue.
     * @param[in] isClean Whether to clean vector after calculate averaged num.
     * @return The averaged num.
     */
    virtual float BlockingGetRate(float defaultValue, bool isClean = true)
    {
        std::unique_lock<std::mutex> lock(vecMutex_);
        if (blockingVec_.empty()) {
            return defaultValue;
        }
        auto successRate =
            std::accumulate(blockingVec_.begin(), blockingVec_.end(), 0.0f) / static_cast<float>(blockingVec_.size());
        if (isClean) {
            blockingVec_.clear();
        }
        return successRate;
    };

    /**
     * @brief Obtains the success rate of all requests. BlockingGetRate will clean vector.
     * @return The success rate string.
     */
    virtual std::string BlockingGetRateToStringAndClean()
    {
        auto successRate = BlockingGetRate(-1.0f);
        auto rateStr = (successRate < 0 ? "" : FormatString("%.3f", successRate));
        return rateStr;
    }

private:
    std::mutex vecMutex_;  // protect blockingVec_
    std::vector<float> blockingVec_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_METRICS_METRICS_BLOCKING_VECTOR_H
