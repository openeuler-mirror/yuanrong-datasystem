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

/** Description: Coordinates deadline-external remote UB port-health verification state. */
#ifndef DATASYSTEM_CLUSTER_UB_HEALTH_REMOTE_UB_PORT_HEALTH_VERIFIER_H
#define DATASYSTEM_CLUSTER_UB_HEALTH_REMOTE_UB_PORT_HEALTH_VERIFIER_H

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include <bthread/mutex.h>

#include "datasystem/common/object_cache/peer_ub_admission.h"

namespace datasystem::cluster {

constexpr size_t REMOTE_UB_PORT_HEALTH_MAX_CONCURRENT_QUERIES = 4;
constexpr uint64_t REMOTE_UB_PORT_HEALTH_UNSUPPORTED_RETRY_INTERVAL_MS = 30'000;
struct RemoteUbQueryTicket {
    HostPort peer;
    std::string incarnation;
    uint64_t generation = 0;
};

struct RemoteUbQueryCompletion {
    bool retryScheduled = false;
    bool evidenceAccepted = false;
};

class RemoteUbPortHealthVerifier {
public:
    RemoteUbPortHealthVerifier();
    ~RemoteUbPortHealthVerifier() = default;

    bool RequestVerification(const HostPort &peer, const std::string &incarnation,
                             uint64_t nowMs);
    std::optional<RemoteUbQueryTicket> TryBeginDue(uint64_t nowMs);
    RemoteUbQueryCompletion Complete(const RemoteUbQueryTicket &ticket,
                                     const std::optional<UbHealthSummary> &summary,
                                     const Status &queryStatus, uint64_t nowMs);
    bool AcceptPassiveRecovery(const UbHealthSummary &summary);
    bool NotifySummaryHint(const UbHealthSummary &summary, uint64_t nowMs);
    void ReconcileTopology(const std::unordered_map<HostPort, std::string> &incarnations);
    std::optional<uint64_t> NextQueryDeadlineMs() const;

private:
    RemoteUbPortHealthVerifier(uint64_t seed, uint64_t retryMinMs, uint64_t retryMaxMs) noexcept;

    struct AcceptedSummaryTransition {
        bool logResponse;
        const char *decision;
    };

    struct PeerState {
        std::string incarnation;
        // Assigned by the verifier-wide monotonic probe counter in TryBeginDue; globally
        // unique within the verifier lifetime, not a per-peer epoch.
        uint64_t generation = 0;
        uint64_t nextQueryMs = 0;
        std::optional<uint64_t> lastQueryMs;
        std::optional<UbPortHealthSummary> lastPortHealth;
        bool inFlight = false;
        bool isolated = false;
        bool summaryHintPending = false;
        bool triggerPending = false;
        bool verificationPending = false;
        std::optional<StatusCode> lastLoggedRetryStatus;
    };

    uint64_t RetryDeadlineMs(const HostPort &peer, const PeerState &state, uint64_t nowMs) const;
    uint64_t NextGenerationLocked();
    void ScheduleAfterCompletion(const HostPort &peer, PeerState &state, uint64_t nowMs,
                                 RemoteUbQueryCompletion &completion) const;
    AcceptedSummaryTransition ApplyAcceptedSummaryLocked(const HostPort &peer, PeerState &state,
                                                         const UbPortHealthSummary &portHealth, uint64_t nowMs,
                                                         RemoteUbQueryCompletion &completion) const;

    const uint64_t seed_;
    const uint64_t retryMinMs_;
    const uint64_t retryMaxMs_;
    uint64_t nextGeneration_ = 0;
    mutable bthread::Mutex mutex_;
    std::unordered_map<HostPort, PeerState> peers_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_UB_HEALTH_REMOTE_UB_PORT_HEALTH_VERIFIER_H
