/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: PB codec for cluster membership lease values.
 */
#include "datasystem/cluster/membership/membership_value_codec.h"

#include <utility>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/coordinator.pb.h"

namespace datasystem::cluster {
namespace {
using MembershipValuePb = coordinator::WorkerServiceInfoPb;
constexpr size_t MAX_MEMBERSHIP_VALUE_BYTES = 64 * 1024;

Status ToProtoState(MemberLifecycleState state, MembershipValuePb::StatePb &statePb)
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

Status FromProtoState(MembershipValuePb::StatePb statePb, MemberLifecycleState &state)
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
        default:
            RETURN_STATUS(K_INVALID, "invalid member lifecycle state");
    }
}
}  // namespace

Status MembershipValueCodec::Encode(const MembershipValue &value, std::string &bytes)
{
    MembershipValuePb::StatePb statePb = MembershipValuePb::UNSPECIFIED;
    RETURN_IF_NOT_OK(ToProtoState(value.lifecycleState, statePb));
    MembershipValuePb pb;
    pb.set_timestamp(value.timestamp);
    pb.set_state(statePb);
    pb.set_host_id(value.hostId);
    pb.set_compatibility_version(value.compatibilityVersion);
    std::string encoded;
    CHECK_FAIL_RETURN_STATUS(pb.SerializeToString(&encoded), K_RUNTIME_ERROR,
                             "serialize membership value proto failed");
    CHECK_FAIL_RETURN_STATUS(encoded.size() <= MAX_MEMBERSHIP_VALUE_BYTES, K_INVALID, "membership value exceeds limit");
    bytes = std::move(encoded);
    return Status::OK();
}

Status MembershipValueCodec::Decode(const std::string &bytes, MembershipValue &value)
{
    MembershipValue decoded;
    if (bytes.size() > MAX_MEMBERSHIP_VALUE_BYTES) {
        value = {};
        RETURN_STATUS(K_INVALID, "membership value exceeds limit");
    }
    MembershipValuePb pb;
    if (!pb.ParseFromString(bytes)) {
        value = {};
        RETURN_STATUS(K_INVALID, "failed to parse membership value proto");
    }
    auto rc = FromProtoState(pb.state(), decoded.lifecycleState);
    if (rc.IsError()) {
        value = {};
        return rc;
    }
    decoded.timestamp = pb.timestamp();
    decoded.hostId = pb.host_id();
    decoded.compatibilityVersion = pb.compatibility_version();
    value = std::move(decoded);
    return Status::OK();
}

}  // namespace datasystem::cluster
