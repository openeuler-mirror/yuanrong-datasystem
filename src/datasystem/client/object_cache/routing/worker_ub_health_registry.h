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

/** Description: Owns the client-wide immutable UB health view used by routing. */
#ifndef DATASYSTEM_CLIENT_ROUTING_WORKER_UB_HEALTH_REGISTRY_H
#define DATASYSTEM_CLIENT_ROUTING_WORKER_UB_HEALTH_REGISTRY_H

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include <bthread/mutex.h>

#include "datasystem/client/object_cache/routing/ub_routing_health.h"
#include "datasystem/common/object_cache/peer_ub_admission.h"
#include "datasystem/protos/cluster_topology.pb.h"

namespace datasystem::client {

class WorkerUbHealthRegistry : public IUbPortHealthObserver {
public:
    using PassiveRecoveryCommit = std::function<bool(const UbHealthSummary &)>;

    WorkerUbHealthRegistry();
    ~WorkerUbHealthRegistry() override = default;

    WorkerUbHealthRegistry(const WorkerUbHealthRegistry &) = delete;
    WorkerUbHealthRegistry &operator=(const WorkerUbHealthRegistry &) = delete;

    void ReconcileTopology(const ::datasystem::ClusterTopologyPb &topology);
    bool ApplySummary(const UbHealthSummary &summary, const std::string &expectedIncarnation);
    bool ApplySummary(const UbHealthSummary &summary, const std::string &expectedIncarnation,
                      const PassiveRecoveryCommit &recoveryCommit, bool &recovered);
    bool ApplyVerifiedSummary(const UbHealthSummary &summary, const std::string &expectedIncarnation);
    bool ApplyLocalClientPortHealth(const UbPortHealthSummary &portHealth);
    void OnUbPortHealthChanged(const UbPortHealthSummary &portHealth) override;

    std::optional<UbHealthSummary> GetSummary(const HostPort &worker) const;
    bool IsVerifiedUnavailable(const HostPort &worker) const;
    std::shared_ptr<const UbRoutingHealthSnapshot> GetRoutingSnapshot() const;

private:
    enum class ApplyResult { REJECTED, ACCEPTED, UPDATED };
    struct ApplyOutcome {
        ApplyResult result = ApplyResult::REJECTED;
        bool recovered = false;
    };
    struct WorkerState;
    struct State;

    void LogRoutingChange(const State &previous, const State &current, const HostPort &worker,
                          const char *source) const;

    void ReconcileTopologyMemberLocked(const std::shared_ptr<const State> &current,
                                       const HostPort &worker,
                                       const ::datasystem::MembershipPb &member,
                                       State &next, WorkerState &workers) const;
    ApplyOutcome ApplySummaryInternal(const UbHealthSummary &summary,
                                      const std::string &expectedIncarnation,
                                      bool verified, const PassiveRecoveryCommit &recoveryCommit = {});
    bool ResolveExpectedIncarnationLocked(const State &current, const HostPort &worker,
                                          const std::string &fallback, std::string &expected) const;

    mutable bthread::Mutex writeMutex_;
    std::shared_ptr<const State> state_;
};

}  // namespace datasystem::client

#endif  // DATASYSTEM_CLIENT_ROUTING_WORKER_UB_HEALTH_REGISTRY_H
