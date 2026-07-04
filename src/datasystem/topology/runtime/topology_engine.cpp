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
#include "datasystem/topology/runtime/topology_engine.h"

#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {

TopologyEngine::TopologyEngine(ICoordinationBackend &backend, const IRoutingAlgorithm &routingAlgorithm)
    : backend_(backend),
      repository_(backend),
      routingAlgorithmView_(&routingAlgorithm, [](const IRoutingAlgorithm *) {}),
      routingView_(std::make_shared<RoutingView>()),
      membershipView_(std::make_shared<MembershipEndpointView>()),
      ownerResolver_(std::make_shared<OwnerEndpointResolver>(routingView_, membershipView_, routingAlgorithmView_)),
      redirectPolicy_(std::make_shared<RedirectPolicy>(routingView_, membershipView_, ownerResolver_)),
      placement_(ownerResolver_, redirectPolicy_, routingView_),
      rebuilder_(*routingView_, repository_, routingAlgorithm),
      readiness_(*routingView_),
      admin_(rebuilder_),
      diagnostics_(eventPump_, rebuilder_, readiness_)
{
}

TopologyEngine::~TopologyEngine()
{
    Shutdown();
}

Status TopologyEngine::Start()
{
    bool alreadyRunning = false;
    RETURN_IF_NOT_OK(BeginStart(alreadyRunning));
    if (alreadyRunning) {
        return Status::OK();
    }
    auto rc = StartComponents();
    if (rc.IsError()) {
        RollbackStart();
        return rc;
    }
    FinishStart();
    LOG(INFO) << "Topology engine started.";
    return Status::OK();
}

void TopologyEngine::Shutdown()
{
    if (!BeginShutdown()) {
        return;
    }
    backend_.SetEventHandler(ICoordinationBackend::EventHandler());
    eventPump_.Shutdown();
    FinishShutdown();
    LOG(INFO) << "Topology engine stopped.";
}

Status TopologyEngine::HandleCoordinationEvent(CoordinationEvent &&event)
{
    return eventPump_.Submit(std::move(event));
}

TopologyEngineState TopologyEngine::GetState() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return state_;
}

PlacementFacade &TopologyEngine::Placement()
{
    return placement_;
}

MembershipEndpointView &TopologyEngine::Membership()
{
    return *membershipView_;
}

TopologyAdmin &TopologyEngine::Admin()
{
    return admin_;
}

const TopologyReadiness &TopologyEngine::Readiness() const
{
    return readiness_;
}

const TopologyDiagnostics &TopologyEngine::Diagnostics() const
{
    return diagnostics_;
}

Status TopologyEngine::BeginStart(bool &alreadyRunning)
{
    alreadyRunning = false;
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ == TopologyEngineState::RUNNING) {
        alreadyRunning = true;
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(state_ == TopologyEngineState::STOPPED, K_TRY_AGAIN, "topology engine is not stopped");
    state_ = TopologyEngineState::STARTING;
    return Status::OK();
}

Status TopologyEngine::StartComponents()
{
    RETURN_IF_NOT_OK(eventPump_.Start([this](CoordinationEvent &&event) {
        return rebuilder_.ApplyCommittedTopologyEvent(event);
    },
    [this]() {
        return rebuilder_.RebuildFromCommittedTopology();
    }));
    backend_.SetEventHandler([this](CoordinationEvent &&event) {
        (void)HandleCoordinationEvent(std::move(event));
    });
    auto rc = rebuilder_.RebuildFromCommittedTopology();
    if (rc.IsError()) {
        backend_.SetEventHandler(ICoordinationBackend::EventHandler());
        eventPump_.Shutdown();
        LOG(ERROR) << "Topology engine initial rebuild failed, status: " << rc.ToString();
    }
    return rc;
}

void TopologyEngine::RollbackStart()
{
    std::lock_guard<std::mutex> lock(mutex_);
    state_ = TopologyEngineState::STOPPED;
    LOG(WARNING) << "Topology engine start rolled back.";
}

void TopologyEngine::FinishStart()
{
    std::lock_guard<std::mutex> lock(mutex_);
    state_ = TopologyEngineState::RUNNING;
}

bool TopologyEngine::BeginShutdown()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ == TopologyEngineState::STOPPED) {
        return false;
    }
    state_ = TopologyEngineState::STOPPING;
    return true;
}

void TopologyEngine::FinishShutdown()
{
    std::lock_guard<std::mutex> lock(mutex_);
    state_ = TopologyEngineState::STOPPED;
}

}  // namespace topology
}  // namespace datasystem
