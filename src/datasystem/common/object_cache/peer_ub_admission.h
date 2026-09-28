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
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <bthread/rwlock.h>

#include "datasystem/common/object_cache/ub_failure_classifier.h"
#include "datasystem/common/object_cache/ub_port_health.h"
#include "datasystem/common/rdma/fast_transport_base.h"

namespace datasystem {

enum class UbAdmissionState { AVAILABLE, SUSPECT, UNAVAILABLE, PROBING };

enum class UbPortHealthVerificationMode : uint8_t { LEGACY = 0, VERIFIED_PORT_HEALTH = 1 };

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
    std::optional<UbPortHealthSummary> portHealth;
    // True only while a confirmed all-down fact owns node-level isolation.
    bool portHealthGoverned = false;
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
    // Absence means an old peer or no observation. A present invalid summary is an explicit UNKNOWN from a new peer.
    // Neither form alone is isolation or recovery evidence.
    std::optional<UbPortHealthSummary> portHealth;
};

inline bool IsSameUbHealthSummary(const UbHealthSummary &lhs, const UbHealthSummary &rhs)
{
    return lhs.worker == rhs.worker && lhs.incarnation == rhs.incarnation && lhs.writable == rhs.writable
           && lhs.state == rhs.state && lhs.reason == rhs.reason && lhs.lastStatusCode == rhs.lastStatusCode
           && lhs.epoch == rhs.epoch && lhs.backoffLevel == rhs.backoffLevel
           && lhs.backoffDeadlineMs == rhs.backoffDeadlineMs && IsSameUbPortHealth(lhs.portHealth, rhs.portHealth);
}

bool MergeUbPortHealth(const std::optional<UbPortHealthSummary> &current,
                       const std::optional<UbPortHealthSummary> &incoming,
                       std::optional<UbPortHealthSummary> &merged);
bool MergeUbHealthSummary(const UbHealthSummary *current, const UbHealthSummary &incoming,
                          UbHealthSummary &merged);

struct UbProbeToken {
    HostPort peer{ "", -1 };
    uint64_t epoch = 0;
};

// Callers serialize these scheduling operations with their existing state lock; evidence policy stays with the owner.
inline bool TryBeginUbProbe(uint64_t nowMs, uint64_t dueMs, bool &inFlight, uint64_t &generation)
{
    if (inFlight || nowMs < dueMs) {
        return false;
    }
    inFlight = true;
    ++generation;
    return true;
}

inline bool MatchesUbProbe(bool inFlight, uint64_t generation, uint64_t token)
{
    return inFlight && generation == token;
}

inline uint64_t UbProbeRetryAt(uint64_t nowMs, uint64_t delayMs)
{
    return nowMs > std::numeric_limits<uint64_t>::max() - delayMs
               ? std::numeric_limits<uint64_t>::max() : nowMs + delayMs;
}

template <typename States, typename Deadline>
std::optional<uint64_t> NextUbProbeDeadline(const States &states, const Deadline &getDeadline)
{
    std::optional<uint64_t> next;
    for (const auto &entry : states) {
        auto due = getDeadline(entry);
        if (due.has_value() && (!next.has_value() || *due < *next)) {
            next = due;
        }
    }
    return next;
}

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
    // Copyable state for owners that already publish immutable snapshots. Mutation is serialized by the owner.
    class Snapshot {
    public:
        ~Snapshot() = default;

        bool Prepare(const UbHealthSummary &summary, const std::string &expectedIncarnation,
                     UbHealthSummary &accepted) const;
        bool Apply(const UbHealthSummary &summary, const std::string &expectedIncarnation);
        const UbHealthSummary *Find(const HostPort &worker) const;
        void Retire(const HostPort &worker, const std::string &incarnation);
        void ReconcileWorkers(const std::unordered_set<HostPort> &workers);
        size_t Size() const
        {
            return summaries_.size();
        }

    private:
        static constexpr size_t MAX_RETIRED_INCARNATIONS_PER_WORKER = 8;
        std::unordered_map<HostPort, UbHealthSummary> summaries_;
        std::unordered_map<HostPort, std::unordered_set<std::string>> retiredIncarnations_;
    };

    UbHealthSummaryCache() = default;
    ~UbHealthSummaryCache() = default;

    bool Apply(const UbHealthSummary &summary, const std::string &expectedIncarnation);
    bool Apply(const UbHealthSummary &summary, const std::string &expectedIncarnation,
               UbHealthSummary &accepted);
    std::optional<UbHealthSummary> Get(const HostPort &worker) const;
    void ReconcileWorkers(const std::unordered_set<HostPort> &workers);
    size_t Size() const;

private:
    mutable bthread::RWLock mutex_;
    Snapshot state_;
};

class PeerUbAdmission : public UrmaLateCompletionObserver,
                        public std::enable_shared_from_this<PeerUbAdmission> {
public:
    using RemotePortHealthVerificationTrigger = std::function<void(const HostPort &)>;

    explicit PeerUbAdmission(
        UbPortHealthVerificationMode verificationMode = UbPortHealthVerificationMode::LEGACY)
        : verificationMode_(verificationMode)
    {
    }
    ~PeerUbAdmission() override = default;

    Status CheckWriteTarget(const HostPort &peer, UbOperationKind op) const;
    Status CheckReadSource(const HostPort &peer) const;
    void ReportOutcome(const UbOpOutcome &outcome);

    void SetRemotePortHealthCapability(const HostPort &peer, bool enabled,
                                       const std::string &incarnation = {});
    void ReconcileRemotePortHealthCapabilities(
        const std::unordered_map<HostPort, std::string> &incarnations);
    void SetRemotePortHealthVerificationTrigger(RemotePortHealthVerificationTrigger trigger);

    /**
     * Apply one valid aggregate port-health fact. Self admission accepts local monitor facts; remote admission accepts
     * query responses, plus newer passive recovery facts only while already isolated. UNKNOWN, invalid, pending, and
     * remote PASSIVE_SUMMARY inputs never change remote admission.
     * Query callers must fence remote responses by the expected Worker incarnation before invoking this method.
     * @return true when a newer port-health fact was accepted.
     */
    bool ApplyPortHealth(const HostPort &subject, const UbPortHealthSummary &summary,
                         UbPortHealthEvidenceSource source);

    void SetSelfWorker(const HostPort &self);
    /** Enable E4/E9 port verification after a usable Provider has been bound. This transition is one-way. */
    void EnableVerifiedPortHealth();
    /** Install the non-blocking self E4 refresh hook. The callback is invoked outside the admission lock. */
    void SetSelfPortHealthRefreshTrigger(std::function<void()> trigger);
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
    static void ApplyProbeRetryBackoff(UbPathState &state, uint64_t nowMs);
    bool IsGlobalWritableLocked(const HostPort &peer) const;
    bool IsReplayLocked(const HostPort &worker, const std::string &incarnation) const;
    void ApplyGlobalRecoveryTransitionLocked(const UbHealthSummary &summary, uint64_t nowMs);
    void RetireWorkerLocked(const HostPort &worker, uint64_t nowMs, uint64_t tombstoneTtlMs);
    void PruneTombstonesLocked(uint64_t nowMs);
    void ReportOutcomeImpl(const UbOpOutcome &outcome, std::optional<LateCompletionFence> fence);
    bool PrepareOutcomeTransition(
        const UbOpOutcome &outcome, UbFailureClass failureClass,
        const std::optional<LateCompletionFence> &fence, bool &changed,
        UbAdmissionState &nextState, std::function<void()> &selfPortHealthRefreshTrigger,
        std::shared_ptr<const RemotePortHealthVerificationTrigger> &remoteVerificationTrigger);
    bool IsLateCompletionFenceCurrentLocked(const HostPort &peer, const LateCompletionFence &fence) const;
    bool UpdatePathStateLocked(const UbOpOutcome &outcome, UbFailureClass failureClass,
                               UbAdmissionState nextState);
    bool RecordPortVerificationTriggerLocked(const UbOpOutcome &outcome, UbFailureClass failureClass);
    bool ApplyPortHealthFactLocked(const HostPort &subject, UbPathState &state, const UbPortHealthSummary &summary,
                                   UbAdmissionState nextState);
    bool IsApplicablePortHealth(const HostPort &subject, const UbPortHealthSummary &summary,
                                UbPortHealthEvidenceSource source) const;
    bool UsesVerifiedPortHealth() const;
    bool IsPortHealthManagedState(const HostPort &subject, const UbPathState &state) const;
    bool HasRemotePortHealthCapabilityLocked(const HostPort &peer) const;
    uint64_t GetOrCreatePeerCompletionGenerationLocked(const HostPort &peer);
    void AdvancePeerCompletionGenerationLocked(const HostPort &peer);

    mutable bthread::RWLock mutex_;
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
    std::atomic<UbPortHealthVerificationMode> verificationMode_;
    std::function<void()> selfPortHealthRefreshTrigger_;
    std::atomic<uint64_t> lateCompletionGeneration_{ 0 };
    std::unordered_map<HostPort, std::string> remotePortHealthPeers_;
    std::shared_ptr<const RemotePortHealthVerificationTrigger> remotePortHealthVerificationTrigger_;
    std::unordered_map<HostPort, uint64_t> peerCompletionGenerations_;
    uint64_t nextPeerCompletionGeneration_{ 0 };
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_OBJECT_CACHE_PEER_UB_ADMISSION_H
