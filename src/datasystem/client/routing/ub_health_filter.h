/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#ifndef DATASYSTEM_CLIENT_ROUTING_UB_HEALTH_FILTER_H
#define DATASYSTEM_CLIENT_ROUTING_UB_HEALTH_FILTER_H

#include <mutex>
#include <optional>
#include <unordered_map>

#include "datasystem/client/routing/i_worker_filter.h"
#include "datasystem/common/object_cache/peer_ub_admission.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "datasystem/protos/object_posix.pb.h"

namespace datasystem::client {
class UbHealthFilter : public IWorkerFilter {
public:
    ~UbHealthFilter() override = default;

    bool ApplySummary(const UbHealthSummary &summary, const std::string &expectedIncarnation);
    void ApplyTopologyIncarnations(const ::datasystem::ClusterTopologyPb &ring);
    bool ReportProviderFailure(const HostPort &provider, const ProviderUbFailureDetailPb &detail);
    bool IsAvailable(const HostPort &addr) const override;
    std::optional<UbPathState> GetLocalObservation(const HostPort &addr) const;

private:
    void ReconcileLocalObservationWithTrustedIncarnationLocked(const HostPort &worker,
                                                               const std::string &incarnation);

    UbHealthSummaryCache cache_;
    PeerUbAdmission localAdmission_;
    // Trusted incarnation updates and Provider failure reports serialize through this mutex so a restart
    // cannot clear evidence learned for the newly published process generation.
    mutable std::mutex incarnationMutex_;
    std::unordered_map<HostPort, std::string> trustedIncarnations_;
    // An empty value means the failure was observed before a trusted incarnation was available.
    std::unordered_map<HostPort, std::string> localObservationIncarnations_;
    bool topologyInitialized_{ false };
};
}  // namespace datasystem::client

#endif  // DATASYSTEM_CLIENT_ROUTING_UB_HEALTH_FILTER_H
