/*
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
 * Description: Worker service information codecs for cluster membership values.
 */
#ifndef DATASYSTEM_TOPOLOGY_MEMBERSHIP_WORKER_NODE_INFO_H
#define DATASYSTEM_TOPOLOGY_MEMBERSHIP_WORKER_NODE_INFO_H

#include <cstdint>
#include <string>

#include "datasystem/protos/coordinator.pb.h"
#include "datasystem/topology/membership/membership_types.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace topology {

enum class WorkerServiceState {
    UNSPECIFIED = 0,
    START,
    RESTART,
    RECOVER,
    READY,
    EXITING,
    DOWNGRADE_RESTART,
};

struct WorkerServiceInfo {
    int64_t timestamp = 0;
    WorkerServiceState state = WorkerServiceState::UNSPECIFIED;
    std::string hostId;
    std::string compatibilityVersion;

    std::string ToString() const;
    static Status FromString(const std::string &str, WorkerServiceInfo &value);

    std::string ToProto() const;
    static Status FromProto(const std::string &str, WorkerServiceInfo &value);
};

Status WorkerServiceStateToString(WorkerServiceState state, std::string &stateStr);
Status StringToWorkerServiceState(const std::string &stateStr, WorkerServiceState &state);

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_MEMBERSHIP_WORKER_NODE_INFO_H
