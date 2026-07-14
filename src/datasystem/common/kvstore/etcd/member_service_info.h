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
 * Description: ETCD membership lease value compatibility codec.
 */
#ifndef DATASYSTEM_COMMON_KVSTORE_ETCD_MEMBER_SERVICE_INFO_H
#define DATASYSTEM_COMMON_KVSTORE_ETCD_MEMBER_SERVICE_INFO_H

#include <cstdint>
#include <string>

#include "datasystem/protos/coordinator.pb.h"
#include "datasystem/cluster/membership/membership_types.h"
#include "datasystem/utils/status.h"

namespace datasystem::cluster {

struct MemberServiceInfo {
    int64_t timestamp = 0;
    MemberLifecycleState state = MemberLifecycleState::UNKNOWN;
    std::string hostId;
    std::string compatibilityVersion;

    std::string ToString() const;
    static Status FromString(const std::string &str, MemberServiceInfo &value);

    std::string ToProto() const;
    static Status FromProto(const std::string &str, MemberServiceInfo &value);
};

Status MemberLifecycleStateToString(MemberLifecycleState state, std::string &stateStr);
Status StringToMemberLifecycleState(const std::string &stateStr, MemberLifecycleState &state);

}  // namespace datasystem::cluster
#endif  // DATASYSTEM_COMMON_KVSTORE_ETCD_MEMBER_SERVICE_INFO_H
