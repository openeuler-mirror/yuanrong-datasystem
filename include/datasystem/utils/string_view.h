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
 * Description: string_view class.
 */

#ifndef DATASYSTEM_UTILS_STRING_VIEW_H
#define DATASYSTEM_UTILS_STRING_VIEW_H

#include <cstring>
#include <string>

namespace datasystem {
class StringView {
public:
    constexpr StringView() noexcept = default;

    constexpr StringView(const StringView &) noexcept = default;

    constexpr StringView(const char *str) : str_(str), len_(str == nullptr ? 0 : std::strlen(str))
    {
    }

    constexpr StringView(const char *str, size_t len) : str_(str), len_(len)
    {
    }

    StringView(const std::string &str) : str_(str.data()), len_(str.size())
    {
    }

    constexpr StringView &operator=(const StringView &) noexcept = default;

    ~StringView() = default;

    constexpr const char *data() const noexcept
    {
        return str_;
    }

    constexpr size_t size() const noexcept
    {
        return len_;
    }

    constexpr bool empty() const noexcept
    {
        return len_ == 0;
    }

private:
    const char *str_ = nullptr;
    size_t len_ = 0;
};
}  // namespace datasystem
#endif  // DATASYSTEM_UTILS_STRING_VIEW_H