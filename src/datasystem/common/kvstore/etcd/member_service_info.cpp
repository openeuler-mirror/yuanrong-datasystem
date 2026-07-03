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
 * Description: Legacy etcd cluster-table member service information codecs.
 */
#include "datasystem/common/kvstore/etcd/member_service_info.h"

#include <sstream>
#include <stdexcept>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {
namespace {
using MemberServiceInfoPb = coordinator::WorkerServiceInfoPb;

constexpr char MEMBER_LIFECYCLE_STATE_STARTING[] = "start";
constexpr char MEMBER_LIFECYCLE_STATE_RESTARTING[] = "restart";
constexpr char MEMBER_LIFECYCLE_STATE_RECOVERING[] = "recover";
constexpr char MEMBER_LIFECYCLE_STATE_READY[] = "ready";
constexpr char MEMBER_LIFECYCLE_STATE_EXITING[] = "exiting";
constexpr char MEMBER_LIFECYCLE_STATE_DOWNGRADE_RESTARTING[] = "d_rst";

Status MemberLifecycleStateToProto(MemberLifecycleState state, MemberServiceInfoPb::StatePb &statePb)
{
    switch (state) {
        case MemberLifecycleState::STARTING:
            statePb = MemberServiceInfoPb::START;
            return Status::OK();
        case MemberLifecycleState::RESTARTING:
            statePb = MemberServiceInfoPb::RESTART;
            return Status::OK();
        case MemberLifecycleState::RECOVERING:
            statePb = MemberServiceInfoPb::RECOVER;
            return Status::OK();
        case MemberLifecycleState::READY:
            statePb = MemberServiceInfoPb::READY;
            return Status::OK();
        case MemberLifecycleState::EXITING:
            statePb = MemberServiceInfoPb::EXITING;
            return Status::OK();
        case MemberLifecycleState::DOWNGRADE_RESTARTING:
            statePb = MemberServiceInfoPb::DOWNGRADE_RESTART;
            return Status::OK();
        default:
            RETURN_STATUS(K_INVALID, "invalid member lifecycle state");
    }
}

Status ProtoToMemberLifecycleState(MemberServiceInfoPb::StatePb statePb, MemberLifecycleState &state)
{
    switch (statePb) {
        case MemberServiceInfoPb::START:
            state = MemberLifecycleState::STARTING;
            return Status::OK();
        case MemberServiceInfoPb::RESTART:
            state = MemberLifecycleState::RESTARTING;
            return Status::OK();
        case MemberServiceInfoPb::RECOVER:
            state = MemberLifecycleState::RECOVERING;
            return Status::OK();
        case MemberServiceInfoPb::READY:
            state = MemberLifecycleState::READY;
            return Status::OK();
        case MemberServiceInfoPb::EXITING:
            state = MemberLifecycleState::EXITING;
            return Status::OK();
        case MemberServiceInfoPb::DOWNGRADE_RESTART:
            state = MemberLifecycleState::DOWNGRADE_RESTARTING;
            return Status::OK();
        case MemberServiceInfoPb::UNSPECIFIED:
        default:
            RETURN_STATUS(K_INVALID, "invalid member lifecycle state");
    }
}

Status ParseMemberServiceTimestamp(const std::string &timestampStr, int64_t &timestamp)
{
    if (timestampStr == "_") {
        timestamp = 0;
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(!timestampStr.empty(), K_INVALID, "member service timestamp is empty");
    size_t pos = 0;
    try {
        auto parsed = std::stoul(timestampStr, &pos);
        CHECK_FAIL_RETURN_STATUS(pos == timestampStr.size(), K_INVALID,
                                 "invalid member service timestamp: " + timestampStr);
        timestamp = static_cast<int64_t>(parsed);
    } catch (const std::exception &) {
        RETURN_STATUS(K_INVALID, "invalid member service timestamp: " + timestampStr);
    }
    return Status::OK();
}
}  // namespace

Status MemberLifecycleStateToString(MemberLifecycleState state, std::string &stateStr)
{
    switch (state) {
        case MemberLifecycleState::STARTING:
            stateStr = MEMBER_LIFECYCLE_STATE_STARTING;
            return Status::OK();
        case MemberLifecycleState::RESTARTING:
            stateStr = MEMBER_LIFECYCLE_STATE_RESTARTING;
            return Status::OK();
        case MemberLifecycleState::RECOVERING:
            stateStr = MEMBER_LIFECYCLE_STATE_RECOVERING;
            return Status::OK();
        case MemberLifecycleState::READY:
            stateStr = MEMBER_LIFECYCLE_STATE_READY;
            return Status::OK();
        case MemberLifecycleState::EXITING:
            stateStr = MEMBER_LIFECYCLE_STATE_EXITING;
            return Status::OK();
        case MemberLifecycleState::DOWNGRADE_RESTARTING:
            stateStr = MEMBER_LIFECYCLE_STATE_DOWNGRADE_RESTARTING;
            return Status::OK();
        default:
            RETURN_STATUS(K_INVALID, "invalid member lifecycle state");
    }
}

Status StringToMemberLifecycleState(const std::string &stateStr, MemberLifecycleState &state)
{
    if (stateStr == MEMBER_LIFECYCLE_STATE_STARTING) {
        state = MemberLifecycleState::STARTING;
    } else if (stateStr == MEMBER_LIFECYCLE_STATE_RESTARTING) {
        state = MemberLifecycleState::RESTARTING;
    } else if (stateStr == MEMBER_LIFECYCLE_STATE_RECOVERING) {
        state = MemberLifecycleState::RECOVERING;
    } else if (stateStr == MEMBER_LIFECYCLE_STATE_READY) {
        state = MemberLifecycleState::READY;
    } else if (stateStr == MEMBER_LIFECYCLE_STATE_EXITING) {
        state = MemberLifecycleState::EXITING;
    } else if (stateStr == MEMBER_LIFECYCLE_STATE_DOWNGRADE_RESTARTING) {
        state = MemberLifecycleState::DOWNGRADE_RESTARTING;
    } else {
        RETURN_STATUS(K_INVALID, "invalid member lifecycle state: " + stateStr);
    }
    return Status::OK();
}

std::string MemberServiceInfo::ToString() const
{
    std::string stateStr;
    auto rc = MemberLifecycleStateToString(state, stateStr);
    if (rc.IsError()) {
        LOG(ERROR) << "MemberServiceInfo::ToString state serialization failed: " << rc.ToString();
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

Status MemberServiceInfo::FromString(const std::string &str, MemberServiceInfo &value)
{
    value = {};
    auto firstPos = str.find(';');
    if (firstPos == std::string::npos) {
        RETURN_STATUS(K_INVALID,
                      FormatString("Invalid member service info: %s, must have at least two fields split by ';'", str));
    }
    auto secondPos = str.find(';', firstPos + 1);
    RETURN_IF_NOT_OK(ParseMemberServiceTimestamp(str.substr(0, firstPos), value.timestamp));
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
    RETURN_IF_NOT_OK(StringToMemberLifecycleState(stateStr, value.state));
    return Status::OK();
}

std::string MemberServiceInfo::ToProto() const
{
    MemberServiceInfoPb info;
    info.set_timestamp(timestamp);
    MemberServiceInfoPb::StatePb statePb = MemberServiceInfoPb::UNSPECIFIED;
    auto rc = MemberLifecycleStateToProto(state, statePb);
    if (rc.IsError()) {
        return "";
    }
    info.set_state(statePb);
    info.set_host_id(hostId);
    info.set_compatibility_version(compatibilityVersion);
    return info.SerializeAsString();
}

Status MemberServiceInfo::FromProto(const std::string &str, MemberServiceInfo &value)
{
    value = {};
    MemberServiceInfoPb info;
    CHECK_FAIL_RETURN_STATUS(info.ParseFromString(str), K_INVALID, "failed to parse member service info proto");
    value.timestamp = info.timestamp();
    RETURN_IF_NOT_OK(ProtoToMemberLifecycleState(info.state(), value.state));
    value.hostId = info.host_id();
    value.compatibilityVersion = info.compatibility_version();
    return Status::OK();
}
}  // namespace topology
}  // namespace datasystem
