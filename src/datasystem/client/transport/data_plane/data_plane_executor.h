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

/** Description: Defines endpoint data-plane execution and scoped connection rebuild. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_DATA_PLANE_DATA_PLANE_EXECUTOR_H
#define DATASYSTEM_CLIENT_TRANSPORT_DATA_PLANE_DATA_PLANE_EXECUTOR_H

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>

#include "datasystem/client/transport/data_plane/data_plane_manager.h"
#include "datasystem/client/transport/transport_advisor.h"
#include "datasystem/client/transport/transport_phase_latency_recorder.h"

namespace datasystem {
namespace client {
class DataPlaneExecutor {
public:
    using Operation = std::function<Status(IDataTransporter &)>;
    using DrainingFallbackHandler = std::function<void(const HostPort &, const Status &)>;

    DataPlaneExecutor(std::shared_ptr<DataPlaneManager> manager, std::shared_ptr<TransportAdvisor> advisor,
                      DrainingFallbackHandler drainingFallbackHandler = nullptr);

    ~DataPlaneExecutor() = default;

    /**
     * @brief Execute one endpoint operation with a bounded connection rebuild or transport fallback.
     * @param[in] workerAddr Target data-worker address.
     * @param[in] operation Operation invoked on the endpoint-scoped transporter.
     * @param[in] traceEnabled Whether to record detailed phase timing.
     * @return K_OK on success; the error code otherwise.
     */
    Status Execute(const HostPort &workerAddr, const Operation &operation, bool traceEnabled = false);

    Status ExecuteForDataLocation(const HostPort &workerAddr, uint64_t locationTopologyVersion,
                                  const Operation &operation, bool traceEnabled = false);

private:
    struct AttemptPlan {
        TransportHint hint;
        size_t attempt;
        const char *connectionPhase;
        const char *transferPhase;
    };

    struct AttemptResult {
        Status status;
        std::shared_ptr<IDataTransporter> transporter;
    };

    AttemptResult ExecuteAttempt(const HostPort &workerAddr, const Operation &operation, const AttemptPlan &plan,
                                 uint64_t locationTopologyVersion, TransportPhaseLatencyRecorder *recorder);

    Status ExecuteFallbacks(const HostPort &workerAddr, const Operation &operation,
                            const std::vector<TransportHint> &fallbackHints, uint64_t locationTopologyVersion,
                            TransportPhaseLatencyRecorder *recorder);

    Status ExecuteImpl(const HostPort &workerAddr, uint64_t locationTopologyVersion,
                       const Operation &operation, bool traceEnabled);

    // Decide whether a failed operation should be retried after a transporter rebuild, perform the
    // rebuild pre-step (ResetDataPlane/Teardown), and set the retry hint. Returns false (no retry)
    // for non-retryable errors. Transport capability/draining fallback is handled separately so each
    // SHM->UB->TCP candidate is attempted at most once.
    bool PrepareRetry(const HostPort &workerAddr, const std::shared_ptr<IDataTransporter> &transporter,
                      const Status &rc, TransportHint hint, TransportHint &retryHint);

    std::shared_ptr<DataPlaneManager> manager_;
    std::shared_ptr<TransportAdvisor> advisor_;
    DrainingFallbackHandler drainingFallbackHandler_;
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_DATA_PLANE_DATA_PLANE_EXECUTOR_H
