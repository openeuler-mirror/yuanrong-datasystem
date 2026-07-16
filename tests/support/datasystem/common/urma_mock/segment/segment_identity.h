/**
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

#ifndef DATASYSTEM_COMMON_URMA_MOCK_SEGMENT_SEGMENT_IDENTITY_H
#define DATASYSTEM_COMMON_URMA_MOCK_SEGMENT_SEGMENT_IDENTITY_H

#include <cstdint>
#include <string>

#include "datasystem/common/urma_mock/abi/urma_abi_compat.h"

namespace datasystem {
namespace urma_mock {

/**
 * @brief Build the registry key for an exported segment.
 * @param[in] seg URMA wire segment identity.
 * @param[in] token Segment token.
 * @return Stable key derived from EID, UASID, token, and remote VA.
 */
std::string BuildSegmentEndpointKey(const urma_seg_t &seg, uint64_t token);

/**
 * @brief Build the registry prefix used to search fallback segment endpoint entries.
 * @param[in] seg URMA wire segment identity.
 * @param[in] token Segment token.
 * @return Stable key prefix derived from EID, UASID, and token.
 */
std::string BuildSegmentEndpointPrefix(const urma_seg_t &seg, uint64_t token);

}  // namespace urma_mock
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_URMA_MOCK_SEGMENT_SEGMENT_IDENTITY_H
