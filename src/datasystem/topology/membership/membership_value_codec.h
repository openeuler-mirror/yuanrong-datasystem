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
#ifndef DATASYSTEM_TOPOLOGY_MEMBERSHIP_MEMBERSHIP_VALUE_CODEC_H
#define DATASYSTEM_TOPOLOGY_MEMBERSHIP_MEMBERSHIP_VALUE_CODEC_H

#include <cstdint>
#include <string>

#include "datasystem/topology/membership/membership_types.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace topology {

struct MembershipValue {
    int64_t timestamp = 0;
    MemberLifecycleState lifecycleState = MemberLifecycleState::UNKNOWN;
    std::string hostId;
    std::string compatibilityVersion;
};

class MembershipValueCodec {
public:
    MembershipValueCodec() = delete;
    ~MembershipValueCodec() = delete;

    /**
     * @brief Encode one membership value into the backend PB payload.
     * @param[in] value Neutral membership value.
     * @param[out] bytes Serialized PB bytes.
     * @return K_OK on success; K_INVALID when lifecycle state is invalid.
     */
    static Status Encode(const MembershipValue &value, std::string &bytes);

    /**
     * @brief Decode one backend PB payload into a neutral membership value.
     * @param[in] bytes Serialized PB bytes.
     * @param[out] value Decoded neutral membership value.
     * @return K_OK on success; K_INVALID for malformed or unsupported payload.
     */
    static Status Decode(const std::string &bytes, MembershipValue &value);
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_MEMBERSHIP_MEMBERSHIP_VALUE_CODEC_H
