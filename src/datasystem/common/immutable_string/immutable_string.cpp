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
 * Description: Immutable string implementation.
 */
#include "datasystem/common/immutable_string/immutable_string.h"

#include "datasystem/common/immutable_string/immutable_string_pool.h"

namespace datasystem {
ImmutableString::ImmutableString(const std::string &val) noexcept
{
    if (!val.empty()) {
        ImmutableStringPool::Instance().Intern(val, strHandle_);
    }
}

ImmutableString::ImmutableString(const char *cStr) : ImmutableString(std::string(cStr))
{
}

std::ostream &operator<<(std::ostream &os, const ImmutableString &obj)
{
    os << obj.ToString();
    return os;
}

size_t ImmutableString::GetHash() const
{
    return strHandle_.ToRefCountStr().GetHash();
}

const RefCountString &ImmutableString::ToRefCountStr() const
{
    return strHandle_.ToRefCountStr();
}

const std::string &ImmutableString::ToString() const
{
    return strHandle_.ToStr();
}

bool ImmutableString::operator==(const ImmutableString &rhs) const
{
    const auto &lhsRCString = strHandle_.ToRefCountStr();
    const auto &rhsRCString = rhs.strHandle_.ToRefCountStr();
    return &lhsRCString == &rhsRCString || lhsRCString == rhsRCString;
}

bool ImmutableString::operator!=(const ImmutableString &rhs) const
{
    return this != &rhs && ToString() != rhs.ToString();
}

bool ImmutableString::operator<(const ImmutableString &rhs) const
{
    return ToString() < rhs.ToString();
}

const char* ImmutableString::Data() const
{
    return ToString().data();
}

std::string::size_type ImmutableString::Size() const
{
    return ToString().size();
}

}  // namespace datasystem

namespace std {
size_t hash<datasystem::ImmutableString>::operator()(const datasystem::ImmutableString &str) const
{
    return str.GetHash();
}

bool equal_to<datasystem::ImmutableString>::operator()(const datasystem::ImmutableString &lhs,
                                                       const datasystem::ImmutableString &rhs) const
{
    return lhs == rhs;
}

bool less<datasystem::ImmutableString>::operator()(const datasystem::ImmutableString &lhs,
                                                   const datasystem::ImmutableString &rhs) const
{
    return lhs < rhs;
}
}  // namespace std