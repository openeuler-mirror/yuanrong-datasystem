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
 * Description: Topology composition root and lifecycle entrypoint.
 */
#ifndef DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_ENGINE_H
#define DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_ENGINE_H

#include <memory>
#include <mutex>

#include "datasystem/topology/algorithm/topology_algorithm.h"
#include "datasystem/topology/coordination_backend/coordination_backend.h"
#include "datasystem/topology/repository/topology_repository.h"
#include "datasystem/topology/routing/membership_endpoint_view.h"
#include "datasystem/topology/routing/owner_endpoint_resolver.h"
#include "datasystem/topology/routing/placement_facade.h"
#include "datasystem/topology/routing/redirect_policy.h"
#include "datasystem/topology/routing/routing_view.h"
#include "datasystem/topology/runtime/coordination_event_pump.h"
#include "datasystem/topology/runtime/topology_admin.h"
#include "datasystem/topology/runtime/topology_diagnostics.h"
#include "datasystem/topology/runtime/topology_readiness.h"
#include "datasystem/topology/runtime/topology_snapshot_rebuilder.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace topology {

enum class TopologyEngineState { STOPPED, STARTING, RUNNING, STOPPING };

class TopologyEngine final {
public:
    /**
     * @brief Create a topology composition root.
     * @param[in] backend Coordination backend that outlives the engine.
     * @param[in] routingAlgorithm Routing algorithm that outlives the engine.
     */
    TopologyEngine(ICoordinationBackend &backend, const IRoutingAlgorithm &routingAlgorithm);
    ~TopologyEngine();
    TopologyEngine(const TopologyEngine &) = delete;
    TopologyEngine &operator=(const TopologyEngine &) = delete;
    TopologyEngine(TopologyEngine &&) = delete;
    TopologyEngine &operator=(TopologyEngine &&) = delete;

    /**
     * @brief Bind backend events, rebuild the initial routing snapshot, and mark the engine running.
     * @return K_OK on success; repository, codec, or algorithm errors are returned unchanged.
     */
    Status Start();

    /**
     * @brief Detach backend events and stop accepting new topology notifications.
     */
    void Shutdown();

    /**
     * @brief Submit one backend event through the engine event pump.
     * @param[in] event Coordination event produced by etcd/coordinator adapters.
     * @return Handler status, or K_NOT_READY after shutdown.
     */
    Status HandleCoordinationEvent(CoordinationEvent &&event);

    /**
     * @brief Return current engine lifecycle state.
     */
    TopologyEngineState GetState() const;

    /**
     * @brief Return the stable placement facade for foreground route queries.
     */
    PlacementFacade &Placement();

    /**
     * @brief Return the endpoint view publisher/query surface paired with placement decisions.
     */
    MembershipEndpointView &Membership();

    /**
     * @brief Return administrative operations that rebuild local runtime state from backend snapshots.
     */
    TopologyAdmin &Admin();

    /**
     * @brief Return readiness view for startup and foreground availability checks.
     */
    const TopologyReadiness &Readiness() const;

    /**
     * @brief Return diagnostic counters for event, rebuild, readiness, and optional task execution state.
     */
    const TopologyDiagnostics &Diagnostics() const;

private:
    Status BeginStart(bool &alreadyRunning);
    Status StartComponents();
    void RollbackStart();
    void FinishStart();
    bool BeginShutdown();
    void FinishShutdown();

    ICoordinationBackend &backend_;
    TopologyRepository repository_;
    std::shared_ptr<const IRoutingAlgorithm> routingAlgorithmView_;
    std::shared_ptr<RoutingView> routingView_;
    std::shared_ptr<MembershipEndpointView> membershipView_;
    std::shared_ptr<OwnerEndpointResolver> ownerResolver_;
    std::shared_ptr<RedirectPolicy> redirectPolicy_;
    PlacementFacade placement_;
    TopologySnapshotRebuilder rebuilder_;
    CoordinationEventPump eventPump_;
    TopologyReadiness readiness_;
    TopologyAdmin admin_;
    TopologyDiagnostics diagnostics_;
    mutable std::mutex mutex_;
    TopologyEngineState state_{ TopologyEngineState::STOPPED };
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_ENGINE_H
