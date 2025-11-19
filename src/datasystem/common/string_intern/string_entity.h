/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: StringEntity reference count declaration.
 */
#ifndef DATASYSTEM_COMMON_STRING_INTERN_STRING_ENTITY_H
#define DATASYSTEM_COMMON_STRING_INTERN_STRING_ENTITY_H

#include <atomic>
#include <cstddef>
#include <string>

#include <tbb/concurrent_hash_map.h>
namespace datasystem {
namespace intern {
class StringEntity {
public:
    explicit StringEntity(std::string val);
    StringEntity() = delete;

    explicit StringEntity(const StringEntity &rStr);

    StringEntity(StringEntity &&rStr) noexcept;

    StringEntity &operator=(const StringEntity &rStr);

    StringEntity &operator=(StringEntity &&rStr) noexcept;

    /**
     * @brief Get the const reference of std::string.
     * @return The the const reference of std::string.
     */
    const std::string &ToStr() const;

    /**
     * @brief Add the reference count of this string.
     * @return The reference count after add.
     */
    int32_t IncRef() const;

    /**
     * @brief Release a reference count of this string.
     * @return Whether the reference count is 0 after release.
     */
    bool DecRef() const;

    /**
     * @brief Add a delete reference count of this string.
     */
    void IncDelRef() const;

    /**
     * @brief Release a delete reference count of this string.
     * @return Whether the reference count is 0 after release.
     */
    bool DecDelRef() const;

    /**
     * @brief Get the hash of string.
     * @return The hash of string.
     */
    size_t GetHash() const;

    /**
     * @brief Get the reference count of this string.
     * @return The reference count.
     */
    size_t GetRef() const;

    bool operator==(const StringEntity &rhs) const;

private:
    /**
     * Only countRef_ may lead to a data rance:
     * 1. Thread A detaches the last reference to x and is preempted.
     * 2. Thread B look for x, find it and attaches a reference to it.
     * 3. Thread A resumes and proceeds with erasing x, leaving a dangling reference in thread B.
     * Here is where the delRef_ count comes into play. This count is
     * incremented when countRef_ changes from 0 to 1, and decremented
     * when a thread is about to check a value for erasure.
     * (Multi threads may check countRef_ is 0 and try to call erase)
     * It can be seen that a value is effectively erasable only when the delRef_ count goes down to 0.
     */
    mutable std::atomic_int32_t delRef_{ 0 };
    mutable std::atomic_int32_t countRef_{ 0 };
    std::string value_;
    size_t hash_;
};
}  // namespace intern
}  // namespace datasystem

namespace tbb {
using datasystem::intern::StringEntity;
template <>
#if TBB_INTERFACE_VERSION >= 12050
struct detail::d1::tbb_hash_compare<StringEntity> {
#else
struct tbb_hash_compare<StringEntity> {
#endif
    static size_t hash(const StringEntity &a)
    {
        return a.GetHash();
    }
    static size_t equal(const StringEntity &a, const StringEntity &b)
    {
        return a.ToStr() == b.ToStr();
    }
};
}  // namespace tbb
#endif
