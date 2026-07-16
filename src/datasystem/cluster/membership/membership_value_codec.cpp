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
 * Description: Codec for cluster membership lease values.
 */
#include "datasystem/cluster/membership/membership_value_codec.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/coordinator.pb.h"

namespace datasystem::cluster {
namespace {
using MembershipValuePb = coordinator::WorkerServiceInfoPb;
constexpr size_t MAX_MEMBERSHIP_VALUE_BYTES = 64 * 1024;

bool LooksLikeLegacyEtcdValue(const std::string &bytes)
{
    const auto delimiter = bytes.find(';');
    if (delimiter == std::string::npos || delimiter == 0) {
        return false;
    }
    if (delimiter == 1 && bytes.front() == '_') {
        return true;
    }
    return std::all_of(bytes.begin(), bytes.begin() + delimiter,
                       [](char value) { return value >= '0' && value <= '9'; });
}

Status ParseLegacyTimestamp(const std::string &bytes, int64_t &timestamp)
{
    if (bytes == "_") {
        timestamp = 0;
        return Status::OK();
    }
    size_t parsedBytes = 0;
    try {
        const auto parsed = std::stoul(bytes, &parsedBytes);
        CHECK_FAIL_RETURN_STATUS(parsedBytes == bytes.size(), K_INVALID, "invalid legacy membership timestamp");
        timestamp = static_cast<int64_t>(parsed);
    } catch (const std::exception &) {
        RETURN_STATUS(K_INVALID, "invalid legacy membership timestamp");
    }
    return Status::OK();
}

Status FromLegacyState(const std::string &stateBytes, MemberLifecycleState &state)
{
    if (stateBytes == "start") {
        state = MemberLifecycleState::STARTING;
    } else if (stateBytes == "restart") {
        state = MemberLifecycleState::RESTARTING;
    } else if (stateBytes == "recover") {
        state = MemberLifecycleState::RECOVERING;
    } else if (stateBytes == "ready") {
        state = MemberLifecycleState::READY;
    } else if (stateBytes == "exiting") {
        state = MemberLifecycleState::EXITING;
    } else if (stateBytes == "d_rst") {
        state = MemberLifecycleState::DOWNGRADE_RESTARTING;
    } else {
        RETURN_STATUS(K_INVALID, "invalid legacy member lifecycle state");
    }
    return Status::OK();
}

Status DecodeLegacyEtcdValue(const std::string &bytes, MembershipValue &value)
{
    MembershipValue decoded;
    const auto firstDelimiter = bytes.find(';');
    const auto secondDelimiter = bytes.find(';', firstDelimiter + 1);
    RETURN_IF_NOT_OK(ParseLegacyTimestamp(bytes.substr(0, firstDelimiter), decoded.timestamp));
    const auto stateBytes = secondDelimiter == std::string::npos
                                ? bytes.substr(firstDelimiter + 1)
                                : bytes.substr(firstDelimiter + 1, secondDelimiter - firstDelimiter - 1);
    RETURN_IF_NOT_OK(FromLegacyState(stateBytes, decoded.lifecycleState));
    if (secondDelimiter != std::string::npos) {
        const auto thirdDelimiter = bytes.find(';', secondDelimiter + 1);
        decoded.hostId = thirdDelimiter == std::string::npos
                             ? bytes.substr(secondDelimiter + 1)
                             : bytes.substr(secondDelimiter + 1, thirdDelimiter - secondDelimiter - 1);
        if (thirdDelimiter != std::string::npos) {
            decoded.compatibilityVersion = bytes.substr(thirdDelimiter + 1);
        }
    }
    value = std::move(decoded);
    return Status::OK();
}

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
    if (LooksLikeLegacyEtcdValue(bytes)) {
        auto rc = DecodeLegacyEtcdValue(bytes, decoded);
        if (rc.IsError()) {
            value = {};
            return rc;
        }
        value = std::move(decoded);
        return Status::OK();
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
