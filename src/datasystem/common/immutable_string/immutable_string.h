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
class ImmutableString {
public:
    ImmutableString() = default;
    ImmutableString(const ImmutableString &val) = default;
    ImmutableString &operator=(const ImmutableString &other) = default;
    ~ImmutableString() = default;

    ImmutableString(const std::string &val) noexcept;
    ImmutableString(const char *cStr);
    /**
     * @brief Get the hash of ImmutableString.
     * @return The hash of ImmutableString.
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

    bool operator==(const ImmutableString &rhs) const;

    bool operator!=(const ImmutableString &rhs) const;

    bool operator<(const ImmutableString &rhs) const;

    /**
     * @brief The operator to convert a ImmutableString to std::string.
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
std::ostream &operator<<(std::ostream &os, const ImmutableString &obj);

}  // namespace datasystem

namespace tbb {
template <>
#if TBB_INTERFACE_VERSION >= 12050
struct detail::d1::tbb_hash_compare<datasystem::ImmutableString> {
#else
struct tbb_hash_compare<datasystem::ImmutableString> {
#endif
    static size_t hash(const datasystem::ImmutableString &a)
    {
        return a.GetHash();
    }
    static size_t equal(const datasystem::ImmutableString &a, const datasystem::ImmutableString &b)
    {
        return a == b;
    }
};
}  // namespace tbb

namespace std {
template <>
struct hash<datasystem::ImmutableString> {
    size_t operator()(const datasystem::ImmutableString &str) const;
};

template <>
struct equal_to<datasystem::ImmutableString> {
    bool operator()(const datasystem::ImmutableString &lhs, const datasystem::ImmutableString &rhs) const;
};

template <>
struct less<datasystem::ImmutableString> {
    bool operator()(const datasystem::ImmutableString &lhs, const datasystem::ImmutableString &rhs) const;
};
}  // namespace std
#endif