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
 * Description: StringEntity with reference count implementation.
 */
#include "datasystem/common/string_intern/string_entity.h"

#include <atomic>

namespace datasystem {
namespace intern {
std::hash<std::string> hasher;

StringEntity::StringEntity(std::string val) : countRef_(0), value_(std::move(val)), hash_(hasher(value_))
{
}

StringEntity::StringEntity(const StringEntity &rStr) : countRef_(0), value_(rStr.value_), hash_(rStr.hash_)
{
}

StringEntity::StringEntity(StringEntity &&rStr) noexcept
    : countRef_(0), value_(std::move(rStr.value_)), hash_(rStr.hash_)
{
}

StringEntity &StringEntity::operator=(const StringEntity &rStr)
{
    countRef_ = 0;
    value_ = rStr.value_;
    hash_ = rStr.hash_;
    return *this;
}

StringEntity &StringEntity::operator=(StringEntity &&rStr) noexcept
{
    countRef_ = 0;
    value_ = std::move(rStr.value_);
    hash_ = rStr.hash_;
    return *this;
}

const std::string &StringEntity::ToStr() const
{
    return value_;
}

int32_t StringEntity::IncRef() const
{
    return ++countRef_;
}

bool StringEntity::DecRef() const
{
    return (--countRef_ == 0);
}

void StringEntity::IncDelRef() const
{
    (void)delRef_.fetch_add(1, std::memory_order_relaxed);
}

bool StringEntity::DecDelRef() const
{
    return --delRef_ == 0;
}

size_t StringEntity::GetHash() const
{
    return hash_;
}

size_t StringEntity::GetRef() const
{
    return countRef_.load(std::memory_order_relaxed);
}

bool StringEntity::operator==(const StringEntity &rhs) const
{
    return this == &rhs || this->value_ == rhs.value_;
}
}  // namespace intern
}  // namespace datasystem
