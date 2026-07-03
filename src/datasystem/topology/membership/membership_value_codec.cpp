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
 * Description: PB codec for topology membership values.
 */
#include "datasystem/topology/membership/membership_value_codec.h"

#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/coordinator.pb.h"

namespace datasystem {
namespace topology {
namespace {
using MembershipValuePb = coordinator::WorkerServiceInfoPb;

Status MemberLifecycleStateToProto(MemberLifecycleState state, MembershipValuePb::StatePb &statePb)
{
    switch (state) {
        case MemberLifecycleState::STARTING:
            statePb = MembershipValuePb::START;
            return Status::OK();
        case MemberLifecycleState::RESTARTING:
            statePb = MembershipValuePb::RESTART;
            return Status::OK();
        case MemberLifecycleState::RECOVERING:
            statePb = MembershipValuePb::RECOVER;
            return Status::OK();
        case MemberLifecycleState::READY:
            statePb = MembershipValuePb::READY;
            return Status::OK();
        case MemberLifecycleState::EXITING:
            statePb = MembershipValuePb::EXITING;
            return Status::OK();
        case MemberLifecycleState::DOWNGRADE_RESTARTING:
            statePb = MembershipValuePb::DOWNGRADE_RESTART;
            return Status::OK();
        default:
            RETURN_STATUS(K_INVALID, "invalid member lifecycle state");
    }
}

Status ProtoToMemberLifecycleState(MembershipValuePb::StatePb statePb, MemberLifecycleState &state)
{
    switch (statePb) {
        case MembershipValuePb::START:
            state = MemberLifecycleState::STARTING;
            return Status::OK();
        case MembershipValuePb::RESTART:
            state = MemberLifecycleState::RESTARTING;
            return Status::OK();
        case MembershipValuePb::RECOVER:
            state = MemberLifecycleState::RECOVERING;
            return Status::OK();
        case MembershipValuePb::READY:
            state = MemberLifecycleState::READY;
            return Status::OK();
        case MembershipValuePb::EXITING:
            state = MemberLifecycleState::EXITING;
            return Status::OK();
        case MembershipValuePb::DOWNGRADE_RESTART:
            state = MemberLifecycleState::DOWNGRADE_RESTARTING;
            return Status::OK();
        case MembershipValuePb::UNSPECIFIED:
        default:
            RETURN_STATUS(K_INVALID, "invalid member lifecycle state");
    }
}
}  // namespace

Status MembershipValueCodec::Encode(const MembershipValue &value, std::string &bytes)
{
    bytes.clear();
    MembershipValuePb pb;
    MembershipValuePb::StatePb statePb = MembershipValuePb::UNSPECIFIED;
    RETURN_IF_NOT_OK(MemberLifecycleStateToProto(value.lifecycleState, statePb));
    pb.set_timestamp(value.timestamp);
    pb.set_state(statePb);
    pb.set_host_id(value.hostId);
    pb.set_compatibility_version(value.compatibilityVersion);
    CHECK_FAIL_RETURN_STATUS(pb.SerializeToString(&bytes), K_RUNTIME_ERROR, "serialize membership value proto failed");
    return Status::OK();
}

Status MembershipValueCodec::Decode(const std::string &bytes, MembershipValue &value)
{
    value = {};
    MembershipValuePb pb;
    CHECK_FAIL_RETURN_STATUS(pb.ParseFromString(bytes), K_INVALID, "failed to parse membership value proto");
    value.timestamp = pb.timestamp();
    RETURN_IF_NOT_OK(ProtoToMemberLifecycleState(pb.state(), value.lifecycleState));
    value.hostId = pb.host_id();
    value.compatibilityVersion = pb.compatibility_version();
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
