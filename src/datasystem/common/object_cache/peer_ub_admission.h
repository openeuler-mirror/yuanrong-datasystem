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

/** Description: Track process-local UB data-provider admission state. */

#ifndef DATASYSTEM_COMMON_OBJECT_CACHE_PEER_UB_ADMISSION_H
#define DATASYSTEM_COMMON_OBJECT_CACHE_PEER_UB_ADMISSION_H

#include <cstdint>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "datasystem/common/object_cache/ub_failure_classifier.h"

namespace datasystem {

enum class UbAdmissionState { AVAILABLE, SUSPECT, UNAVAILABLE, PROBING };

struct UbPathState {
    UbAdmissionState state = UbAdmissionState::AVAILABLE;
    Status lastStatus;
    UbFailureClass lastFailureClass = UbFailureClass::SUCCESS;
    uint64_t epoch = 0;
    uint32_t backoffLevel = 0;
    uint64_t backoffDeadlineMs = 0;
    std::optional<int> providerStatus;
    std::optional<int> cqeStatus;
};

struct UbHealthSummary {
    HostPort worker{ "", -1 };
    std::string incarnation;
    bool writable = true;
    UbAdmissionState state = UbAdmissionState::AVAILABLE;
    UbFailureClass reason = UbFailureClass::SUCCESS;
    StatusCode lastStatusCode = StatusCode::K_OK;
    uint64_t epoch = 0;
    uint32_t backoffLevel = 0;
    uint64_t backoffDeadlineMs = 0;
};

struct UbProbeToken {
    HostPort peer{ "", -1 };
    uint64_t epoch = 0;
};

struct PeerUbAdmissionStats {
    size_t localStates = 0;
    size_t globalSummaries = 0;
    size_t latestIncarnations = 0;
    size_t retiredWorkerBuckets = 0;
    size_t pendingDepartures = 0;
    size_t replayTombstones = 0;
};

class UbHealthSummaryCache {
public:
    UbHealthSummaryCache() = default;
    ~UbHealthSummaryCache() = default;

    bool Apply(const UbHealthSummary &summary, const std::string &expectedIncarnation);
    std::optional<UbHealthSummary> Get(const HostPort &worker) const;
    void ReconcileWorkers(const std::unordered_set<HostPort> &workers);
    size_t Size() const;

private:
    static constexpr size_t MAX_RETIRED_INCARNATIONS_PER_WORKER = 8;

    mutable std::shared_mutex mutex_;
    std::unordered_map<HostPort, UbHealthSummary> summaries_;
    std::unordered_map<HostPort, std::unordered_set<std::string>> retiredIncarnations_;
};

class PeerUbAdmission {
public:
    PeerUbAdmission() = default;
    ~PeerUbAdmission() = default;

    Status CheckWriteTarget(const HostPort &peer, UbOperationKind op) const;
    Status CheckReadSource(const HostPort &peer) const;
    void ReportOutcome(const UbOpOutcome &outcome);
    void SetSelfWorker(const HostPort &self);
    void ReplaceGlobalSummaries(const std::vector<UbHealthSummary> &summaries);
    void InitializeProbing(const HostPort &peer, uint64_t nowMs);
    std::optional<UbProbeToken> TryBeginProbe(const HostPort &peer, uint64_t nowMs);
    bool CompleteProbe(const UbProbeToken &token, const Status &status, uint64_t nowMs,
                       bool requireGlobalAvailable = true);
    std::optional<HostPort> NextProbeCandidate(uint64_t nowMs) const;
    std::optional<uint64_t> NextProbeDeadlineMs() const;
    void ReconcileTopologyWorkers(const std::unordered_set<HostPort> &workers, uint64_t nowMs,
                                  uint64_t cleanupGraceMs);
    void PruneExpiredTopologyState(uint64_t nowMs);
    UbHealthSummary BuildSelfHealthSummary(const HostPort &self) const;
    std::optional<UbPathState> GetState(const HostPort &peer) const;
    PeerUbAdmissionStats GetStats() const;
    void ClearLocalState(const HostPort &peer);

private:
    struct RetiredWorkerTombstone {
        std::unordered_set<std::string> incarnations;
        uint64_t expiresAtMs = 0;
    };

    static constexpr uint32_t MAX_PROBE_BACKOFF_LEVEL = 6;
    static constexpr uint64_t PROBE_BASE_DELAY_MS = 1'000;
    static constexpr size_t MAX_REPLAY_TOMBSTONES = 8'192;

    static bool ShouldBlock(const UbPathState &state);
    static Status BuildUnavailableStatus(const HostPort &peer, StatusCode code);
    static uint64_t ProbeBackoffMs(uint32_t level);
    bool IsGlobalWritableLocked(const HostPort &peer) const;
    bool IsReplayLocked(const HostPort &worker, const std::string &incarnation) const;
    void ApplyGlobalRecoveryTransitionLocked(const UbHealthSummary &summary, uint64_t nowMs);
    void RetireWorkerLocked(const HostPort &worker, uint64_t nowMs, uint64_t tombstoneTtlMs);
    void PruneTombstonesLocked(uint64_t nowMs);

    mutable std::shared_mutex mutex_;
    std::unordered_map<HostPort, UbPathState> states_;
    std::unordered_map<HostPort, UbHealthSummary> globalSummaries_;
    std::unordered_map<HostPort, std::string> latestGlobalIncarnations_;
    std::unordered_map<HostPort, std::unordered_set<std::string>> retiredGlobalIncarnations_;
    std::unordered_set<HostPort> topologyWorkers_;
    std::unordered_map<HostPort, uint64_t> departedWorkers_;
    std::unordered_map<HostPort, RetiredWorkerTombstone> replayTombstones_;
    uint64_t nextTombstoneExpiryMs_ = 0;
    bool topologyInitialized_ = false;
    UbFailureClassifier classifier_;
    HostPort self_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_OBJECT_CACHE_PEER_UB_ADMISSION_H
