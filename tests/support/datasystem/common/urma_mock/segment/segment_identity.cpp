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

#include "datasystem/common/urma_mock/segment/segment_identity.h"

#include <iomanip>
#include <sstream>

namespace datasystem {
namespace urma_mock {
namespace {
constexpr int K_HEX_BYTE_WIDTH = 2;

std::string HexEid(const urma_eid_t &eid)
{
    std::ostringstream os;
    os << std::hex << std::setfill('0');
    for (auto byte : eid.raw) {
        os << std::setw(K_HEX_BYTE_WIDTH) << static_cast<unsigned int>(byte);
    }
    return os.str();
}
}  // namespace

std::string BuildSegmentEndpointKey(const urma_seg_t &seg, uint64_t token)
{
    return HexEid(seg.ubva.eid) + ":" + std::to_string(seg.ubva.uasid) + ":" + std::to_string(token) + ":"
           + std::to_string(seg.ubva.va);
}

std::string BuildSegmentEndpointPrefix(const urma_seg_t &seg, uint64_t token)
{
    return HexEid(seg.ubva.eid) + ":" + std::to_string(seg.ubva.uasid) + ":" + std::to_string(token) + ":";
}

}  // namespace urma_mock
}  // namespace datasystem
