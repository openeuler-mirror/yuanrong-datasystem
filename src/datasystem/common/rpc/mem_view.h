/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Memory view helper
 */
#ifndef DATASYSTEM_COMMON_UTIL_MEM_VIEW_H
#define DATASYSTEM_COMMON_UTIL_MEM_VIEW_H

#include <cstdint>
#include <cstdlib>

namespace datasystem {
/**
 * @brief A MemView wraps a const pointer in memory and its size.
 */
class MemView {
public:
    /**
     * Constructors
     */
    MemView();
    MemView(const void *data, size_t sz);
    MemView(const MemView &) = default;
    MemView &operator=(const MemView &) = default;
    MemView(MemView &&other) noexcept;
    MemView &operator=(MemView &&other) noexcept;

    auto Data() const
    {
        return data_;
    }

    auto Size() const
    {
        return size_;
    }

    MemView &operator+=(size_t n);

private:
    const void *data_;
    size_t size_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_UTIL_MEM_VIEW_H
