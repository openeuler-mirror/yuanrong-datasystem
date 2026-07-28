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

#include "datasystem/common/object_cache/peer_ub_admission.h"

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"

namespace datasystem {

Status PeerUbAdmission::CheckWriteTarget(const HostPort &peer, UbOperationKind op) const
{
    (void)op;
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = states_.find(peer);
    if (it == states_.end() || !ShouldBlock(it->second)) {
        return Status::OK();
    }
    return BuildUnavailableStatus(peer, StatusCode::K_URMA_WORKER_UNAVAILABLE);
}

Status PeerUbAdmission::CheckReadSource(const HostPort &peer) const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = states_.find(peer);
    if (it == states_.end() || !ShouldBlock(it->second)) {
        return Status::OK();
    }
    return BuildUnavailableStatus(peer, StatusCode::K_URMA_DATA_WORKER_UNAVAILABLE);
}

void PeerUbAdmission::ReportOutcome(const UbOpOutcome &outcome)
{
    auto failureClass = classifier_.Classify(outcome);
    if (failureClass == UbFailureClass::SUCCESS || failureClass == UbFailureClass::LOCAL_RESOURCE_PRESSURE
        || failureClass == UbFailureClass::NON_UB_FAILURE) {
        return;
    }

    const auto nextState =
        failureClass == UbFailureClass::TIMEOUT_SUSPECT ? UbAdmissionState::SUSPECT : UbAdmissionState::UNAVAILABLE;
    bool stateChanged = false;
    {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        auto &state = states_[outcome.peer];
        stateChanged = state.state != nextState;
        state.lastStatus = outcome.status;
        state.lastFailureClass = failureClass;
        state.state = nextState;
        ++state.epoch;
    }
    if (!stateChanged) {
        return;
    }
    if (nextState == UbAdmissionState::SUSPECT) {
        LOG(INFO) << "UB admission marked peer SUSPECT, peer=" << outcome.peer
                  << ", statusCode=" << outcome.status.GetCode();
    } else {
        LOG(WARNING) << "UB admission marked peer UNAVAILABLE, peer=" << outcome.peer
                     << ", statusCode=" << outcome.status.GetCode()
                     << ", failureClass=" << static_cast<int>(failureClass);
    }
}

std::optional<UbPathState> PeerUbAdmission::GetState(const HostPort &peer) const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = states_.find(peer);
    return it == states_.end() ? std::nullopt : std::optional<UbPathState>{ it->second };
}

bool PeerUbAdmission::ShouldBlock(const UbPathState &state)
{
    return state.state == UbAdmissionState::UNAVAILABLE || state.state == UbAdmissionState::PROBING;
}

Status PeerUbAdmission::BuildUnavailableStatus(const HostPort &peer, StatusCode code)
{
    return Status(code, FormatString("UB data plane unavailable for peer %s", peer.ToString()));
}

}  // namespace datasystem
