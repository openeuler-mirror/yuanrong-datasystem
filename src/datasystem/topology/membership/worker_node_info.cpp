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
#include "datasystem/topology/membership/worker_node_info.h"

#include <sstream>
#include <stdexcept>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {
namespace {
using WorkerServiceInfoPb = coordinator::WorkerServiceInfoPb;

constexpr char WORKER_SERVICE_STATE_START[] = "start";
constexpr char WORKER_SERVICE_STATE_RESTART[] = "restart";
constexpr char WORKER_SERVICE_STATE_RECOVER[] = "recover";
constexpr char WORKER_SERVICE_STATE_READY[] = "ready";
constexpr char WORKER_SERVICE_STATE_EXITING[] = "exiting";
constexpr char WORKER_SERVICE_STATE_DOWNGRADE_RESTART[] = "d_rst";
constexpr char WORKER_SERVICE_STATE_UNSPECIFIED[] = "unspecified";

Status WorkerServiceStateToProto(WorkerServiceState state, WorkerServiceInfoPb::StatePb &statePb)
{
    switch (state) {
        case WorkerServiceState::START:
            statePb = WorkerServiceInfoPb::START;
            return Status::OK();
        case WorkerServiceState::RESTART:
            statePb = WorkerServiceInfoPb::RESTART;
            return Status::OK();
        case WorkerServiceState::RECOVER:
            statePb = WorkerServiceInfoPb::RECOVER;
            return Status::OK();
        case WorkerServiceState::READY:
            statePb = WorkerServiceInfoPb::READY;
            return Status::OK();
        case WorkerServiceState::EXITING:
            statePb = WorkerServiceInfoPb::EXITING;
            return Status::OK();
        case WorkerServiceState::DOWNGRADE_RESTART:
            statePb = WorkerServiceInfoPb::DOWNGRADE_RESTART;
            return Status::OK();
        default:
            RETURN_STATUS(K_INVALID, "invalid worker service state");
    }
}

Status ProtoToWorkerServiceState(WorkerServiceInfoPb::StatePb statePb, WorkerServiceState &state)
{
    switch (statePb) {
        case WorkerServiceInfoPb::START:
            state = WorkerServiceState::START;
            return Status::OK();
        case WorkerServiceInfoPb::RESTART:
            state = WorkerServiceState::RESTART;
            return Status::OK();
        case WorkerServiceInfoPb::RECOVER:
            state = WorkerServiceState::RECOVER;
            return Status::OK();
        case WorkerServiceInfoPb::READY:
            state = WorkerServiceState::READY;
            return Status::OK();
        case WorkerServiceInfoPb::EXITING:
            state = WorkerServiceState::EXITING;
            return Status::OK();
        case WorkerServiceInfoPb::DOWNGRADE_RESTART:
            state = WorkerServiceState::DOWNGRADE_RESTART;
            return Status::OK();
        case WorkerServiceInfoPb::UNSPECIFIED:
        default:
            RETURN_STATUS(K_INVALID, "invalid worker service state");
    }
}

Status ParseWorkerServiceTimestamp(const std::string &timestampStr, int64_t &timestamp)
{
    if (timestampStr == "_") {
        timestamp = 0;
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(!timestampStr.empty(), K_INVALID, "worker service timestamp is empty");
    size_t pos = 0;
    try {
        auto parsed = std::stoul(timestampStr, &pos);
        CHECK_FAIL_RETURN_STATUS(pos == timestampStr.size(), K_INVALID,
                                 "invalid worker service timestamp: " + timestampStr);
        timestamp = static_cast<int64_t>(parsed);
    } catch (const std::exception &) {
        RETURN_STATUS(K_INVALID, "invalid worker service timestamp: " + timestampStr);
    }
    return Status::OK();
}
}  // namespace

Status WorkerServiceStateToString(WorkerServiceState state, std::string &stateStr)
{
    switch (state) {
        case WorkerServiceState::START:
            stateStr = WORKER_SERVICE_STATE_START;
            return Status::OK();
        case WorkerServiceState::RESTART:
            stateStr = WORKER_SERVICE_STATE_RESTART;
            return Status::OK();
        case WorkerServiceState::RECOVER:
            stateStr = WORKER_SERVICE_STATE_RECOVER;
            return Status::OK();
        case WorkerServiceState::READY:
            stateStr = WORKER_SERVICE_STATE_READY;
            return Status::OK();
        case WorkerServiceState::EXITING:
            stateStr = WORKER_SERVICE_STATE_EXITING;
            return Status::OK();
        case WorkerServiceState::DOWNGRADE_RESTART:
            stateStr = WORKER_SERVICE_STATE_DOWNGRADE_RESTART;
            return Status::OK();
        default:
            RETURN_STATUS(K_INVALID, "invalid worker service state");
    }
}

Status StringToWorkerServiceState(const std::string &stateStr, WorkerServiceState &state)
{
    if (stateStr == WORKER_SERVICE_STATE_START) {
        state = WorkerServiceState::START;
    } else if (stateStr == WORKER_SERVICE_STATE_RESTART) {
        state = WorkerServiceState::RESTART;
    } else if (stateStr == WORKER_SERVICE_STATE_RECOVER) {
        state = WorkerServiceState::RECOVER;
    } else if (stateStr == WORKER_SERVICE_STATE_READY) {
        state = WorkerServiceState::READY;
    } else if (stateStr == WORKER_SERVICE_STATE_EXITING) {
        state = WorkerServiceState::EXITING;
    } else if (stateStr == WORKER_SERVICE_STATE_DOWNGRADE_RESTART) {
        state = WorkerServiceState::DOWNGRADE_RESTART;
    } else {
        RETURN_STATUS(K_INVALID, "invalid worker service state: " + stateStr);
    }
    return Status::OK();
}

std::string WorkerServiceInfo::ToString() const
{
    std::string stateStr;
    auto rc = WorkerServiceStateToString(state, stateStr);
    if (rc.IsError()) {
        LOG(ERROR) << "WorkerServiceInfo::ToString state serialization failed: " << rc.ToString();
        return "";
    }
    std::stringstream ss;
    ss << timestamp << ";" << stateStr;
    if (!hostId.empty() || !compatibilityVersion.empty()) {
        ss << ";" << hostId;
    }
    if (!compatibilityVersion.empty()) {
        ss << ";" << compatibilityVersion;
    }
    return ss.str();
}

Status WorkerServiceInfo::FromString(const std::string &str, WorkerServiceInfo &value)
{
    value = {};
    auto firstPos = str.find(';');
    if (firstPos == std::string::npos) {
        RETURN_STATUS(K_INVALID,
                      FormatString("Invalid worker service info: %s, must have at least two fields split by ';'", str));
    }
    auto secondPos = str.find(';', firstPos + 1);
    RETURN_IF_NOT_OK(ParseWorkerServiceTimestamp(str.substr(0, firstPos), value.timestamp));
    std::string stateStr;
    if (secondPos == std::string::npos) {
        stateStr = str.substr(firstPos + 1);
    } else {
        stateStr = str.substr(firstPos + 1, secondPos - firstPos - 1);
        auto thirdPos = str.find(';', secondPos + 1);
        if (thirdPos == std::string::npos) {
            value.hostId = str.substr(secondPos + 1);
        } else {
            value.hostId = str.substr(secondPos + 1, thirdPos - secondPos - 1);
            value.compatibilityVersion = str.substr(thirdPos + 1);
        }
    }
    RETURN_IF_NOT_OK(StringToWorkerServiceState(stateStr, value.state));
    return Status::OK();
}

std::string WorkerServiceInfo::ToProto() const
{
    WorkerServiceInfoPb info;
    info.set_timestamp(timestamp);
    WorkerServiceInfoPb::StatePb statePb = WorkerServiceInfoPb::UNSPECIFIED;
    auto rc = WorkerServiceStateToProto(state, statePb);
    if (rc.IsError()) {
        return "";
    }
    info.set_state(statePb);
    info.set_host_id(hostId);
    info.set_compatibility_version(compatibilityVersion);
    return info.SerializeAsString();
}

Status WorkerServiceInfo::FromProto(const std::string &str, WorkerServiceInfo &value)
{
    value = {};
    WorkerServiceInfoPb info;
    CHECK_FAIL_RETURN_STATUS(info.ParseFromString(str), K_INVALID, "failed to parse worker service info proto");
    value.timestamp = info.timestamp();
    RETURN_IF_NOT_OK(ProtoToWorkerServiceState(info.state(), value.state));
    value.hostId = info.host_id();
    value.compatibilityVersion = info.compatibility_version();
    return Status::OK();
}
}  // namespace topology
}  // namespace datasystem
