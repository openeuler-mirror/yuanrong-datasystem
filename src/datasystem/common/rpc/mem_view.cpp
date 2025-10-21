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

#include "datasystem/common/rpc/mem_view.h"
#include <algorithm>

namespace datasystem {
MemView::MemView() : data_(nullptr), size_(0)
{
}

MemView::MemView(const void *data, size_t sz) : data_(data), size_(sz)
{
}

MemView::MemView(MemView &&other) noexcept : data_(other.data_), size_(other.size_)
{
    other.data_ = nullptr;
    other.size_ = 0;
}

MemView &MemView::operator=(MemView &&other) noexcept
{
    if (this != &other) {
        data_ = other.data_;
        size_ = other.size_;
        other.data_ = nullptr;
        other.size_ = 0;
    }
    return *this;
}

MemView &MemView::operator+=(size_t n)
{
    const auto shift = std::min(n, size_);
    data_ = static_cast<const uint8_t *>(data_) + shift;
    size_ -= shift;
    return *this;
}
}  // namespace datasystem
