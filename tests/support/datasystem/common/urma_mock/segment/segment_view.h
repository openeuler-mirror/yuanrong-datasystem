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

#ifndef DATASYSTEM_COMMON_URMA_MOCK_SEGMENT_SEGMENT_VIEW_H
#define DATASYSTEM_COMMON_URMA_MOCK_SEGMENT_SEGMENT_VIEW_H

#include <cstdint>
#include <memory>

#include "datasystem/common/urma_mock/objects/mock_seg.h"

namespace datasystem {
namespace urma_mock {

/**
 * @brief Get the address base used for range checks.
 * @param[in] seg Segment to inspect.
 * @return Address base as uintptr_t, or 0 for null segments.
 */
uintptr_t GetSegmentAddressBase(const std::shared_ptr<MockSeg> &seg);

/**
 * @brief Get the address base used for range checks.
 * @param[in] seg Segment to inspect.
 * @return Address base as uintptr_t, or 0 for null segments.
 */
uintptr_t GetSegmentAddressBase(const MockSeg *seg);

/**
 * @brief Check whether an address falls inside a segment.
 * @param[in] seg Segment to inspect.
 * @param[in] address Address to test.
 * @return true if the address is in the segment range.
 */
bool ContainsAddress(const std::shared_ptr<MockSeg> &seg, uintptr_t address);

/**
 * @brief Check whether an address falls inside a segment.
 * @param[in] seg Segment to inspect.
 * @param[in] address Address to test.
 * @return true if the address is in the segment range.
 */
bool ContainsAddress(const MockSeg *seg, uintptr_t address);

}  // namespace urma_mock
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_URMA_MOCK_SEGMENT_SEGMENT_VIEW_H
