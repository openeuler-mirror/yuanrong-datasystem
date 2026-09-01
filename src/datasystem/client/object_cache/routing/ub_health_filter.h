/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#ifndef DATASYSTEM_CLIENT_ROUTING_UB_HEALTH_FILTER_H
#define DATASYSTEM_CLIENT_ROUTING_UB_HEALTH_FILTER_H

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "datasystem/client/object_cache/routing/i_worker_filter.h"
#include "datasystem/common/object_cache/peer_ub_admission.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "datasystem/protos/object_posix.pb.h"

namespace datasystem::client {
struct ProviderUbRecoveryCandidate {
    UbProbeToken token;
    std::string expectedIncarnation;
};

struct WriteTargetUbRecoveryCandidate {
    UbProbeToken token;
    std::string expectedIncarnation;
};

class UbHealthFilter : public IWorkerFilter {
public:
    UbHealthFilter();
    ~UbHealthFilter() override = default;

    bool ApplySummary(const UbHealthSummary &summary, const std::string &expectedIncarnation);
    void ApplyTopologyIncarnations(const ::datasystem::ClusterTopologyPb &ring);
    bool ReportProviderFailure(const HostPort &provider, const ProviderUbFailureDetailPb &detail);
    bool ReportWriteTargetFailure(const HostPort &worker, const Status &status,
                                  std::optional<int> providerStatus, std::optional<int> cqeStatus);
    uint64_t CaptureWriteTargetCompletionGeneration(const HostPort &worker);
    void ReportLateWriteTargetFailure(const UrmaLateCompletion &completion, uint64_t peerToken) noexcept;
    bool IsAvailable(const HostPort &addr) const override;
    bool IsWriteTargetAvailable(const HostPort &addr) const;
    std::vector<HostPort> GetUnavailableWriteTargets() const;
    std::optional<UbPathState> GetWriteTargetObservation(const HostPort &addr) const;
    std::optional<UbPathState> GetLocalObservation(const HostPort &addr) const;
    std::optional<ProviderUbRecoveryCandidate> TryBeginProviderRecovery(uint64_t nowMs);
    bool CompleteProviderRecovery(const ProviderUbRecoveryCandidate &candidate,
                                  const std::optional<UbHealthSummary> &summary, const Status &probeStatus,
                                  uint64_t nowMs);
    std::optional<uint64_t> NextProviderRecoveryDeadlineMs() const;
    std::optional<WriteTargetUbRecoveryCandidate> TryBeginWriteTargetRecovery(uint64_t nowMs);
    bool CompleteWriteTargetRecovery(const WriteTargetUbRecoveryCandidate &candidate, const Status &probeStatus,
                                     uint64_t nowMs);
    std::optional<uint64_t> NextWriteTargetRecoveryDeadlineMs() const;

private:
    using WriteTargetCompletionGenerations = std::unordered_map<HostPort, uint64_t>;

    void ReconcileLocalObservationWithTrustedIncarnationLocked(const HostPort &worker,
                                                               const std::string &incarnation);
    void PublishWriteTargetCompletionGenerationsLocked(const std::unordered_set<HostPort> &workers);
    void RefreshWriteTargetCompletionGenerationLocked(const HostPort &worker);

    UbHealthSummaryCache cache_;
    PeerUbAdmission localAdmission_;
    std::shared_ptr<PeerUbAdmission> writeTargetAdmission_;
    // Trusted incarnation updates and Provider failure reports serialize through this mutex so a restart
    // cannot clear evidence learned for the newly published process generation.
    mutable std::mutex incarnationMutex_;
    std::unordered_map<HostPort, std::string> trustedIncarnations_;
    // An empty value means the failure was observed before a trusted incarnation was available.
    std::unordered_map<HostPort, std::string> localObservationIncarnations_;
    std::unordered_map<HostPort, std::string> writeTargetObservationIncarnations_;
    std::shared_ptr<const WriteTargetCompletionGenerations> writeTargetCompletionGenerations_;
    std::atomic<size_t> writeTargetObservationCount_{ 0 };
    bool topologyInitialized_{ false };
};
}  // namespace datasystem::client

#endif  // DATASYSTEM_CLIENT_ROUTING_UB_HEALTH_FILTER_H
