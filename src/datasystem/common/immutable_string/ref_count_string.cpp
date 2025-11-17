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
 * Description: String class with reference count implementation.
 */
#include "datasystem/common/immutable_string/ref_count_string.h"

#include <atomic>

#include "datasystem/common/immutable_string/immutable_string_pool.h"

namespace datasystem {

std::hash<std::string> hasher;

static RefCountString EMPTY_REF_COUNT_STRING = RefCountString("");

RefCountString::RefCountString(std::string val) : countRef_(0), value_(std::move(val)), hash_(hasher(value_))
{
}

RefCountString::RefCountString(const RefCountString &rStr) : countRef_(0), value_(rStr.value_), hash_(rStr.hash_)
{
}

RefCountString::RefCountString(RefCountString &&rStr) noexcept
    : countRef_(0), value_(std::move(rStr.value_)), hash_(rStr.hash_)
{
}

RefCountString &RefCountString::operator=(const RefCountString &rStr)
{
    countRef_ = 0;
    value_ = rStr.value_;
    hash_ = rStr.hash_;
    return *this;
}

RefCountString &RefCountString::operator=(RefCountString &&rStr) noexcept
{
    countRef_ = 0;
    value_ = std::move(rStr.value_);
    hash_ = rStr.hash_;
    return *this;
}

const std::string &RefCountString::ToStr() const
{
    return value_;
}

int32_t RefCountString::AddRef() const
{
    return ++countRef_;
}

bool RefCountString::ReleaseRef() const
{
    return (--countRef_ == 0);
}

void RefCountString::addDeleteRef() const
{
    (void)delRef_.fetch_add(1, std::memory_order_relaxed);
}
bool RefCountString::ReleaseDelRef() const
{
    return --delRef_ == 0;
}

size_t RefCountString::GetHash() const
{
    return hash_;
}

size_t RefCountString::GetRef() const
{
    return countRef_.load(std::memory_order_relaxed);
}

RefCountStringHandle::RefCountStringHandle() : ptr_(&EMPTY_REF_COUNT_STRING)
{
}

RefCountStringHandle::RefCountStringHandle(const RefCountString &str) : ptr_(&str)
{
    if (ptr_ != nullptr && ptr_->AddRef() == 1) {
        ptr_->addDeleteRef();
    }
};

RefCountStringHandle::RefCountStringHandle(const RefCountStringHandle &handle) : ptr_(handle.ptr_)
{
    if (ptr_ != nullptr) {
        ptr_->AddRef();
    }
};

const std::string &RefCountStringHandle::ToStr() const
{
    if (ptr_ != nullptr) {
        return ptr_->ToStr();
    }
    return default_;
};

const RefCountString &RefCountStringHandle::ToRefCountStr() const
{
    return *ptr_;
};

RefCountStringHandle::~RefCountStringHandle()
{
    if (ptr_ != nullptr && ptr_->ReleaseRef()) {
        ImmutableStringPool::Instance().Erase(*this);
    };
}

bool RefCountString::operator==(const RefCountString &rhs) const
{
    return this == &rhs || this->value_ == rhs.value_;
}

RefCountStringHandle &RefCountStringHandle::operator=(const RefCountStringHandle &handle)
{
    if (this != &handle) {
        ptr_ = handle.ptr_;
        if (ptr_ != nullptr) {
            ptr_->AddRef();
        }
    }
    return *this;
}

const std::string RefCountStringHandle::default_ = "";

}  // namespace datasystem
