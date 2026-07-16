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

#include "datasystem/common/urma_mock/segment/segment_view.h"

#include <cstdint>

#include "datasystem/common/urma_mock/objects/mock_seg.h"

namespace datasystem {
namespace urma_mock {

uintptr_t GetSegmentAddressBase(const std::shared_ptr<MockSeg> &seg)
{
    if (seg == nullptr) {
        return uintptr_t{ 0 };
    }
    auto remoteVa = static_cast<uintptr_t>(seg->GetRemoteVa());
    return remoteVa != 0 ? remoteVa : reinterpret_cast<uintptr_t>(seg->GetPtr());
}

uintptr_t GetSegmentAddressBase(const MockSeg *seg)
{
    if (seg == nullptr) {
        return uintptr_t{ 0 };
    }
    auto remoteVa = static_cast<uintptr_t>(seg->GetRemoteVa());
    return remoteVa != 0 ? remoteVa : reinterpret_cast<uintptr_t>(seg->GetPtr());
}

bool ContainsAddress(const std::shared_ptr<MockSeg> &seg, uintptr_t address)
{
    auto base = GetSegmentAddressBase(seg);
    if (seg == nullptr || base == 0 || address == 0 || base > UINTPTR_MAX - seg->GetSize()) {
        return false;
    }
    return address >= base && address < base + seg->GetSize();
}

bool ContainsAddress(const MockSeg *seg, uintptr_t address)
{
    auto base = GetSegmentAddressBase(seg);
    if (seg == nullptr || base == 0 || address == 0 || base > UINTPTR_MAX - seg->GetSize()) {
        return false;
    }
    return address >= base && address < base + seg->GetSize();
}

}  // namespace urma_mock
}  // namespace datasystem
