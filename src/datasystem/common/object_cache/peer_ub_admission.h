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

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "datasystem/common/object_cache/ub_failure_classifier.h"
#include "datasystem/common/rdma/fast_transport_base.h"

namespace datasystem {

enum class UbAdmissionState { AVAILABLE, SUSPECT, UNAVAILABLE, PROBING };

struct UbPathState {
    UbAdmissionState state = UbAdmissionState::AVAILABLE;
    Status lastStatus;
    UbFailureClass lastFailureClass = UbFailureClass::SUCCESS;
    uint64_t epoch = 0;
    uint32_t backoffLevel = 0;
    uint64_t backoffDeadlineMs = 0;
    bool probeInFlight = false;
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
    size_t peerCompletionGenerations = 0;
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

class PeerUbAdmission : public UrmaLateCompletionObserver,
                        public std::enable_shared_from_this<PeerUbAdmission> {
public:
    PeerUbAdmission() = default;
    ~PeerUbAdmission() override = default;

    Status CheckWriteTarget(const HostPort &peer, UbOperationKind op) const;
    Status CheckReadSource(const HostPort &peer) const;
    void ReportOutcome(const UbOpOutcome &outcome);
    void SetSelfWorker(const HostPort &self);
    void ReplaceGlobalSummaries(const std::vector<UbHealthSummary> &summaries);
    void InitializeVerification(const HostPort &peer, uint64_t nowMs);
    std::optional<UbProbeToken> TryBeginProbe(const HostPort &peer, uint64_t nowMs);
    bool CancelProbe(const UbProbeToken &token, uint64_t nowMs);
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
    std::optional<UrmaLateCompletionContext> BuildLateCompletionContext(
        UbOperationKind operation, const std::optional<HostPort> &remotePeer = std::nullopt);
    void OnLateUrmaCompletion(const UrmaLateCompletion &completion, uint64_t ownerToken,
                              uint64_t peerToken) noexcept override;

private:
    enum class LateCompletionScope { LOCAL_SENDER, REMOTE_PEER };

    struct LateCompletionFence {
        LateCompletionScope scope;
        uint64_t generation;
    };

    struct RetiredWorkerTombstone {
        std::unordered_set<std::string> incarnations;
        uint64_t expiresAtMs = 0;
    };

    static constexpr uint32_t MAX_PROBE_BACKOFF_LEVEL = 6;
    static constexpr uint64_t PROBE_BASE_DELAY_MS = 1'000;
    static constexpr size_t MAX_REPLAY_TOMBSTONES = 8'192;
    static constexpr uint64_t LATE_COMPLETION_OPERATION_BITS = 8;
    static constexpr uint64_t LATE_COMPLETION_OPERATION_MASK = (1ULL << LATE_COMPLETION_OPERATION_BITS) - 1;

    static bool ShouldBlock(const UbPathState &state);
    static Status BuildUnavailableStatus(const HostPort &peer, StatusCode code);
    static uint64_t ProbeBackoffMs(uint32_t level);
    bool IsGlobalWritableLocked(const HostPort &peer) const;
    bool IsReplayLocked(const HostPort &worker, const std::string &incarnation) const;
    void ApplyGlobalRecoveryTransitionLocked(const UbHealthSummary &summary, uint64_t nowMs);
    void RetireWorkerLocked(const HostPort &worker, uint64_t nowMs, uint64_t tombstoneTtlMs);
    void PruneTombstonesLocked(uint64_t nowMs);
    void ReportOutcomeImpl(const UbOpOutcome &outcome, std::optional<LateCompletionFence> fence);
    bool IsLateCompletionFenceCurrentLocked(const HostPort &peer, const LateCompletionFence &fence) const;
    bool UpdatePathStateLocked(const UbOpOutcome &outcome, UbFailureClass failureClass,
                               UbAdmissionState nextState);
    uint64_t GetOrCreatePeerCompletionGenerationLocked(const HostPort &peer);
    void AdvancePeerCompletionGenerationLocked(const HostPort &peer);

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
    std::atomic<uint64_t> lateCompletionGeneration_{ 0 };
    std::unordered_map<HostPort, uint64_t> peerCompletionGenerations_;
    uint64_t nextPeerCompletionGeneration_{ 0 };
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_OBJECT_CACHE_PEER_UB_ADMISSION_H
