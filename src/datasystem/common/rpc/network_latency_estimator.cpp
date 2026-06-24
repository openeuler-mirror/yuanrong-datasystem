/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#include "datasystem/common/rpc/network_latency_estimator.h"

namespace datasystem {

NetworkLatencyEstimator &NetworkLatencyEstimator::Instance()
{
    static NetworkLatencyEstimator instance;
    return instance;
}

void NetworkLatencyEstimator::Update(int64_t residualUs)
{
    if (residualUs <= 0 || residualUs > ABNORMAL_THRESHOLD_US) {
        return;
    }
    int64_t expected = 0;
    if (ewmaUs_.compare_exchange_strong(expected, residualUs, std::memory_order_acq_rel,
                                        std::memory_order_acquire)) {
        initialized_.store(true, std::memory_order_release);
        return;
    }
    int64_t current = expected;
    int64_t next;
    do {
        next = static_cast<int64_t>(ALPHA * residualUs + (1.0 - ALPHA) * current);
    } while (!ewmaUs_.compare_exchange_weak(current, next, std::memory_order_acq_rel, std::memory_order_acquire));
}

int64_t NetworkLatencyEstimator::GetDeductUs() const
{
    if (!initialized_.load(std::memory_order_acquire)) {
        return 0;
    }
    int64_t val = ewmaUs_.load(std::memory_order_acquire);
    return val > MAX_DEDUCT_US ? MAX_DEDUCT_US : val;
}

void NetworkLatencyEstimator::Reset()
{
    ewmaUs_.store(0, std::memory_order_release);
    initialized_.store(false, std::memory_order_release);
}

}  // namespace datasystem
