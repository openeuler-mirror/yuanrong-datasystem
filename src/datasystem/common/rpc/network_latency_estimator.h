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

/**
 * Description: EWMA-based estimator of RPC residual budget for deadline safety-margin deduction.
 */
#ifndef DATASYSTEM_COMMON_RPC_NETWORK_LATENCY_ESTIMATOR_H
#define DATASYSTEM_COMMON_RPC_NETWORK_LATENCY_ESTIMATOR_H

#include <atomic>
#include <cstdint>

namespace datasystem {

/**
 * @brief NetworkLatencyEstimator singleton class estimating RPC residual budget
 * via an EWMA, used to deduct a safety margin from RPC timeouts. Only near-timeout
 * RPCs (residual < 500ms) feed the EWMA; the deduction is capped at MAX_DEDUCT_US.
 */
class NetworkLatencyEstimator {
public:
    /** @brief Get the singleton NetworkLatencyEstimator instance. @return NetworkLatencyEstimator instance. */
    static NetworkLatencyEstimator &Instance();

    /**
     * @brief Feed a new RPC residual-time sample into the EWMA.
     * @param[in] residualUs Remaining RPC budget after a call, in microseconds.
     */
    void Update(int64_t residualUs);

    /**
     * @brief Get the latency margin to deduct from the next RPC timeout.
     * @return Deduction in microseconds, capped at MAX_DEDUCT_US; 0 if never updated.
     */
    int64_t GetDeductUs() const;

    /** @brief Reset the estimator to the uninitialized state. */
    void Reset();

    NetworkLatencyEstimator(const NetworkLatencyEstimator&) = delete;
    NetworkLatencyEstimator& operator=(const NetworkLatencyEstimator&) = delete;
    NetworkLatencyEstimator(NetworkLatencyEstimator&&) = delete;
    NetworkLatencyEstimator& operator=(NetworkLatencyEstimator&&) = delete;

private:
    NetworkLatencyEstimator() = default;
    ~NetworkLatencyEstimator() = default;

    static constexpr double ALPHA = 0.2;
    static constexpr int64_t MAX_DEDUCT_US = 200;
    /** Only residuals below this threshold feed the EWMA, filtering out fast RPCs with large unused budgets. */
    static constexpr int64_t ABNORMAL_THRESHOLD_US = 500'000;

    std::atomic<int64_t> ewmaUs_{0};
    std::atomic<bool> initialized_{false};
};

}  // namespace datasystem
#endif
