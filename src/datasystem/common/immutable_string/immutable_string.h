/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Immutable string declaration.
 */
#ifndef DATASYSTEM_COMMON_IMMUTABLE_STRING_H
#define DATASYSTEM_COMMON_IMMUTABLE_STRING_H

#include <tbb/concurrent_unordered_set.h>

#include "datasystem/common/immutable_string/ref_count_string.h"

namespace datasystem {
using ImmutableString = std::string;
class ImmutableStringImpl {
public:
    ImmutableStringImpl() = default;
    ImmutableStringImpl(const ImmutableStringImpl &val) = default;
    ImmutableStringImpl &operator=(const ImmutableStringImpl &other) = default;
    ~ImmutableStringImpl() = default;

    ImmutableStringImpl(const std::string &val) noexcept;
    ImmutableStringImpl(const char *cStr);
    /**
     * @brief Get the hash of ImmutableStringImpl.
     * @return The hash of ImmutableStringImpl.
     */
    size_t GetHash() const;

    /**
     * @brief Get the const reference of RefCountString.
     * @return The the const reference of RefCountString.
     */
    const RefCountString &ToRefCountStr() const;

    /**
     * @brief Get the const reference of std::string.
     * @return The the const reference of std::string.
     */
    const std::string &ToString() const;

    bool operator==(const ImmutableStringImpl &rhs) const;

    bool operator!=(const ImmutableStringImpl &rhs) const;

    bool operator<(const ImmutableStringImpl &rhs) const;

    /**
     * @brief The operator to convert a ImmutableStringImpl to std::string.
     * @return The the const reference of std::string.
     */
    operator const std::string &() const
    {
        return strHandle_.ToStr();
    }

    const char* Data() const;

    std::string::size_type Size() const;

private:
    RefCountStringHandle strHandle_;
};
std::ostream &operator<<(std::ostream &os, const ImmutableStringImpl &obj);

}  // namespace datasystem

namespace tbb {
template <>
#if TBB_INTERFACE_VERSION >= 12050
struct detail::d1::tbb_hash_compare<datasystem::ImmutableStringImpl> {
#else
struct tbb_hash_compare<datasystem::ImmutableStringImpl> {
#endif
    static size_t hash(const datasystem::ImmutableStringImpl &a)
    {
        return a.GetHash();
    }
    static size_t equal(const datasystem::ImmutableStringImpl &a, const datasystem::ImmutableStringImpl &b)
    {
        return a == b;
    }
};
}  // namespace tbb

namespace std {
template <>
struct hash<datasystem::ImmutableStringImpl> {
    size_t operator()(const datasystem::ImmutableStringImpl &str) const;
};

template <>
struct equal_to<datasystem::ImmutableStringImpl> {
    bool operator()(const datasystem::ImmutableStringImpl &lhs, const datasystem::ImmutableStringImpl &rhs) const;
};

template <>
struct less<datasystem::ImmutableStringImpl> {
    bool operator()(const datasystem::ImmutableStringImpl &lhs, const datasystem::ImmutableStringImpl &rhs) const;
};
}  // namespace std
#endif