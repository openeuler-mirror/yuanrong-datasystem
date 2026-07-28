// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * Description: Coordinator raft state machine event callbacks.
 */
#include "datasystem/coordinator/raft/coordinator_raft_state_machine.h"

#include <cerrno>
#include <exception>
#include <utility>

#include "datasystem/common/log/log.h"

namespace datasystem::coordinator {
namespace {
Status ConvertButilStatus(const butil::Status &status)
{
    if (status.ok()) {
        return Status::OK();
    }
    return Status(K_RUNTIME_ERROR, status.error_str());
}

void LogCallbackFailure()
{
    LOG(ERROR) << kCoordinatorRaftCallbackFailureMarker;
}

void ReportCallbackFailure(const std::function<void(Status)> &onError)
{
    if (!onError) {
        return;
    }
    try {
        onError(Status(K_RUNTIME_ERROR, kCoordinatorRaftCallbackFailureMarker));
    } catch (const std::exception &e) {
        LOG(ERROR) << kCoordinatorRaftCallbackFailureMarker << ": " << e.what();
    } catch (...) {
        LogCallbackFailure();
    }
}
}  // namespace

CoordinatorRaftStateMachine::CoordinatorRaftStateMachine(CoordinatorRaftEventCallbacks callbacks)
    : callbacks_(std::move(callbacks))
{
}

void CoordinatorRaftStateMachine::on_apply(braft::Iterator &iterator)
{
    if (!iterator.valid()) {
        return;
    }

    butil::Status status(ENOTSUP, "Coordinator raft management log apply is not supported yet");
    iterator.set_error_and_rollback(1, &status);
    return;
}

void CoordinatorRaftStateMachine::on_leader_start(int64_t term)
{
    try {
        if (callbacks_.onLeaderStart) {
            callbacks_.onLeaderStart(term);
        }
    } catch (const std::exception &e) {
        LOG(ERROR) << kCoordinatorRaftCallbackFailureMarker << ": " << e.what();
        ReportCallbackFailure(callbacks_.onError);
    } catch (...) {
        ReportCallbackFailure(callbacks_.onError);
    }
}

void CoordinatorRaftStateMachine::on_leader_stop(const butil::Status &status)
{
    try {
        if (callbacks_.onLeaderStop) {
            auto callbackStatus = ConvertButilStatus(status);
            callbacks_.onLeaderStop(std::move(callbackStatus));
        }
    } catch (const std::exception &e) {
        LOG(ERROR) << kCoordinatorRaftCallbackFailureMarker << ": " << e.what();
        ReportCallbackFailure(callbacks_.onError);
    } catch (...) {
        ReportCallbackFailure(callbacks_.onError);
    }
}

void CoordinatorRaftStateMachine::on_configuration_committed(const braft::Configuration &configuration, int64_t index)
{
    try {
        if (!callbacks_.onConfigurationCommitted) {
            return;
        }

        std::vector<braft::PeerId> raftPeers;
        configuration.list_peers(&raftPeers);
        std::vector<std::string> peers;
        peers.reserve(raftPeers.size());
        for (const auto &peer : raftPeers) {
            peers.emplace_back(peer.to_string());
        }
        callbacks_.onConfigurationCommitted(std::move(peers), index);
    } catch (const std::exception &e) {
        LOG(ERROR) << kCoordinatorRaftCallbackFailureMarker << ": " << e.what();
        ReportCallbackFailure(callbacks_.onError);
    } catch (...) {
        ReportCallbackFailure(callbacks_.onError);
    }
}

void CoordinatorRaftStateMachine::on_error(const braft::Error &error)
{
    try {
        if (callbacks_.onError) {
            auto callbackStatus = ConvertButilStatus(error.status());
            callbacks_.onError(std::move(callbackStatus));
        }
    } catch (const std::exception &e) {
        LOG(ERROR) << kCoordinatorRaftCallbackFailureMarker << ": " << e.what();
    } catch (...) {
        LogCallbackFailure();
    }
}

void CoordinatorRaftStateMachine::on_shutdown()
{
    try {
        if (callbacks_.onShutdown) {
            callbacks_.onShutdown();
        }
    } catch (const std::exception &e) {
        LOG(ERROR) << kCoordinatorRaftCallbackFailureMarker << ": " << e.what();
        ReportCallbackFailure(callbacks_.onError);
    } catch (...) {
        ReportCallbackFailure(callbacks_.onError);
    }
}

}  // namespace datasystem::coordinator
