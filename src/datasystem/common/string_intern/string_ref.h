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
 * Description: StringRef implementation.
 */
#ifndef DATASYSTEM_COMMON_STRING_INTERN_STRING_REF_H
#define DATASYSTEM_COMMON_STRING_INTERN_STRING_REF_H

#include "datasystem/common/string_intern/string_entity.h"
#include "datasystem/common/string_intern/string_ptr.h"
#include "datasystem/common/string_intern/string_pool.h"

namespace datasystem {
namespace intern {
enum class KeyType : size_t { OTHER, OBJECT_KEY, CLIENT_KEY, SHM_KEY };

template <auto I>
class StringRef {
public:
    static StringRef<I> Intern(const std::string &str);

    StringRef() = default;

    StringRef(StringPtr handle) : handle_(handle)
    {
    }

    StringRef(const StringRef &other) noexcept : handle_(other.handle_)
    {
        handle_.IncRef();
    }
    StringRef &operator=(const StringRef &other) noexcept
    {
        if (this != &other) {
            handle_ = other.ptr_;
            handle_.IncRef();
        }
        return *this;
    }

    StringRef(StringRef &&other) noexcept
    {
        std::swap(handle_, other.handle_);
    }

    StringRef &operator=(StringRef &&other) noexcept
    {
        std::swap(handle_, other.handle_);
        other.Clear();
        return *this;
    }

    ~StringRef()
    {
        Clear();
    }

    void Clear()
    {
        if (handle_.DecRef()) {
            StringPool<I>::Instance().Erase(handle_);
        }
        handle_ = StringPtr();
    }

    template <auto K = I, typename std::enable_if_t<K == KeyType::OTHER, bool> = true>
    StringRef(const std::string &val) noexcept
    {
        if (!val.empty()) {
            handle_ = StringPool<I>::Instance().Intern(val);
        }
    }

    template <auto K = I, typename std::enable_if_t<K == KeyType::OTHER, bool> = true>
    StringRef(const char *cStr) : StringRef(std::string(cStr))
    {
    }

    /**
     * @brief Get the hash of StringRef.
     * @return The hash of StringRef.
     */
    size_t GetHash() const
    {
        return handle_.GetHash();
    }

    /**
     * @brief Get the const reference of std::string.
     * @return The the const reference of std::string.
     */
    const std::string &ToString() const
    {
        return handle_.ToStr();
    }

    bool operator==(const StringRef &rhs) const
    {
        return this == &rhs || handle_.GetEntity() == rhs.handle_.GetEntity();
    }

    bool operator!=(const StringRef &rhs) const
    {
        return this != &rhs && ToString() != rhs.ToString();
    }

    bool operator<(const StringRef &rhs) const
    {
        return ToString() < rhs.ToString();
    }

    /**
     * @brief The operator to convert a StringRef to std::string.
     * @return The the const reference of std::string.
     */
    operator const std::string &() const
    {
        return handle_.ToStr();
    }

    const char *Data() const
    {
        return ToString().data();
    }

    std::string::size_type Size() const
    {
        return ToString().size();
    }

private:
    StringPtr handle_;
};

template <auto I>
inline StringRef<I> StringRef<I>::Intern(const std::string &str)
{
    if (str.empty()) {
        return StringRef<I>(StringPtr());
    }
    return StringRef<I>(StringPool<I>::Instance().Intern(str));
}

template <auto I>
std::ostream &operator<<(std::ostream &os, const StringRef<I> &obj)
{
    os << obj.ToString();
    return os;
}

template <auto I, typename S>
inline std::string operator+(S &&lhs, const StringRef<I> &rhs)
{
    return std::forward<S>(lhs) + rhs.ToString();
}

template <auto I, typename S>
inline std::string operator+(const StringRef<I> &lhs, S &&rhs)
{
    return lhs.ToString() + std::forward<S>(rhs);
}
}  // namespace intern

using ObjectKey = intern::StringRef<intern::KeyType::OBJECT_KEY>;
using ObjectKeyPool = intern::StringPool<intern::KeyType::OBJECT_KEY>;

using ClientKey = intern::StringRef<intern::KeyType::CLIENT_KEY>;
using ClientKeyPool = intern::StringPool<intern::KeyType::CLIENT_KEY>;

using ShmKey = intern::StringRef<intern::KeyType::SHM_KEY>;
using ShmKeyPool = intern::StringPool<intern::KeyType::SHM_KEY>;

using OtherKey = intern::StringRef<intern::KeyType::OTHER>;
using OtherKeyPool = intern::StringPool<intern::KeyType::OTHER>;

}  // namespace datasystem

#if TBB_INTERFACE_VERSION >= 12050
#define STRING_REF_IMPL_FOR_TBB(key)                                                            \
    template <>                                                                                 \
    struct detail::d1::tbb_hash_compare<StringRef<KeyType::key>> {                              \
        static size_t hash(const StringRef<KeyType::key> &a)                                    \
        {                                                                                       \
            return a.GetHash();                                                                 \
        }                                                                                       \
        static size_t equal(const StringRef<KeyType::key> &a, const StringRef<KeyType::key> &b) \
        {                                                                                       \
            return a == b;                                                                      \
        }                                                                                       \
    }
#else
#define STRING_REF_IMPL_FOR_TBB(key)                                                            \
    template <>                                                                                 \
    struct tbb_hash_compare<StringRef<KeyType::key>> {                                          \
        static size_t hash(const StringRef<KeyType::key> &a)                                    \
        {                                                                                       \
            return a.GetHash();                                                                 \
        }                                                                                       \
        static size_t equal(const StringRef<KeyType::key> &a, const StringRef<KeyType::key> &b) \
        {                                                                                       \
            return a == b;                                                                      \
        }                                                                                       \
    }
#endif

#define STRING_REF_IMPL_FOR_STD(key)                                                                  \
    template <>                                                                                       \
    struct hash<StringRef<KeyType::key>> {                                                            \
        size_t operator()(const StringRef<KeyType::key> &str) const                                   \
        {                                                                                             \
            return str.GetHash();                                                                     \
        }                                                                                             \
    };                                                                                                \
                                                                                                      \
    template <>                                                                                       \
    struct equal_to<StringRef<KeyType::key>> {                                                        \
        bool operator()(const StringRef<KeyType::key> &lhs, const StringRef<KeyType::key> &rhs) const \
        {                                                                                             \
            return lhs == rhs;                                                                        \
        }                                                                                             \
    };                                                                                                \
                                                                                                      \
    template <>                                                                                       \
    struct less<StringRef<KeyType::key>> {                                                            \
        bool operator()(const StringRef<KeyType::key> &lhs, const StringRef<KeyType::key> &rhs) const \
        {                                                                                             \
            return lhs < rhs;                                                                         \
        }                                                                                             \
    }

namespace tbb {
using datasystem::intern::KeyType;
using datasystem::intern::StringRef;
STRING_REF_IMPL_FOR_TBB(OBJECT_KEY);
STRING_REF_IMPL_FOR_TBB(CLIENT_KEY);
STRING_REF_IMPL_FOR_TBB(SHM_KEY);
STRING_REF_IMPL_FOR_TBB(OTHER);
}  // namespace tbb

namespace std {
using datasystem::intern::KeyType;
using datasystem::intern::StringRef;
STRING_REF_IMPL_FOR_STD(OBJECT_KEY);
STRING_REF_IMPL_FOR_STD(CLIENT_KEY);
STRING_REF_IMPL_FOR_STD(SHM_KEY);
STRING_REF_IMPL_FOR_STD(OTHER);
}  // namespace std
#endif
