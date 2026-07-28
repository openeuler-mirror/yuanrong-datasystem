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
#ifndef DATASYSTEM_COORDINATOR_RAFT_COORDINATOR_RAFT_STATE_MACHINE_H
#define DATASYSTEM_COORDINATOR_RAFT_COORDINATOR_RAFT_STATE_MACHINE_H

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include <braft/raft.h>

#include "datasystem/utils/status.h"

namespace datasystem::coordinator {

inline constexpr char kCoordinatorRaftCallbackFailureMarker[] = "Coordinator raft callback failure";

struct CoordinatorRaftEventCallbacks {
    std::function<void(int64_t)> onLeaderStart;
    std::function<void(Status)> onLeaderStop;
    std::function<void(std::vector<std::string>, int64_t)> onConfigurationCommitted;
    std::function<void(Status)> onError;
    std::function<void()> onShutdown;
};

class CoordinatorRaftStateMachine final : public braft::StateMachine {
public:
    explicit CoordinatorRaftStateMachine(CoordinatorRaftEventCallbacks callbacks);
    ~CoordinatorRaftStateMachine() override = default;

    void on_apply(braft::Iterator &iterator) override;
    void on_leader_start(int64_t term) override;
    void on_leader_stop(const butil::Status &status) override;
    void on_configuration_committed(const braft::Configuration &configuration, int64_t index) override;
    void on_error(const braft::Error &error) override;
    void on_shutdown() override;

private:
    CoordinatorRaftEventCallbacks callbacks_;
};

}  // namespace datasystem::coordinator

#endif  // DATASYSTEM_COORDINATOR_RAFT_COORDINATOR_RAFT_STATE_MACHINE_H
