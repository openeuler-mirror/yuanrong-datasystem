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
 * Description: optional class.
 */

#ifndef DATASYSTEM_UTILS_OPTIONAL_H
#define DATASYSTEM_UTILS_OPTIONAL_H

#include <utility>

namespace datasystem {
template <typename T>
class Optional {
public:
    constexpr Optional() = default;

    Optional(const Optional &) = default;
    Optional &operator=(const Optional &) = default;

    Optional(Optional &&other)  noexcept = default;
    Optional &operator=(Optional &&)  noexcept = default;

    template <typename... Args>
    explicit Optional(Args &&... args) : init_(true), val_(std::forward<Args>(args)...)
    {
    }

    ~Optional() = default;

    explicit operator bool() const noexcept
    {
        return init_;
    }

    T &value()
    {
        return val_;
    }

    T *operator->()
    {
        return &val_;
    }

    T &operator*()
    {
        return val_;
    }

    const T &value() const
    {
        return val_;
    }

    const T *operator->() const
    {
        return &val_;
    }

    const T &operator*() const
    {
        return val_;
    }

private:
    bool init_ = false;
    T val_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_UTILS_OPTIONAL_H