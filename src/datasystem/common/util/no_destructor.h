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

/**
 * Description: Utility wrapper for process-lifetime objects that must not run
 * static destruction.
 */
#ifndef DATASYSTEM_COMMON_UTIL_NO_DESTRUCTOR_H
#define DATASYSTEM_COMMON_UTIL_NO_DESTRUCTOR_H

#include <new>
#include <type_traits>
#include <utility>

namespace datasystem {

template <typename T>
class NoDestructor {
public:
    template <typename... Args>
    explicit NoDestructor(Args &&...args)
    {
        new (&storage_) T(std::forward<Args>(args)...);
    }

    NoDestructor(const NoDestructor &) = delete;
    NoDestructor &operator=(const NoDestructor &) = delete;

    ~NoDestructor() = default;

    T &operator*()
    {
        return *reinterpret_cast<T *>(&storage_);
    }

    const T &operator*() const
    {
        return *reinterpret_cast<const T *>(&storage_);
    }

    T *operator->()
    {
        return reinterpret_cast<T *>(&storage_);
    }

    const T *operator->() const
    {
        return reinterpret_cast<const T *>(&storage_);
    }

private:
    typename std::aligned_storage<sizeof(T), alignof(T)>::type storage_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_UTIL_NO_DESTRUCTOR_H
