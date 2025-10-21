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
 * Description: The SensitiveValue implement.
 */

#include "datasystem/utils/sensitive_value.h"

#include <securec.h>
#include <cstring>

#include "datasystem/common/util/format.h"
#include "datasystem/common/log/log.h"

namespace datasystem {
SensitiveValue::SensitiveValue(const char *str)
{
    SetData(str, str == nullptr ? 0 : std::strlen(str));
}

SensitiveValue::SensitiveValue(const std::string &str)
{
    SetData(str.data(), str.length());
}

SensitiveValue::SensitiveValue(const char *str, size_t size)
{
    SetData(str, size);
}

SensitiveValue::SensitiveValue(std::unique_ptr<char[]> data, size_t size) : data_(std::move(data)), size_(size)
{
}

SensitiveValue::SensitiveValue(SensitiveValue &&other) noexcept : data_(std::move(other.data_)), size_(other.size_)
{
    other.size_ = 0;
}

SensitiveValue::SensitiveValue(const SensitiveValue &other)
{
    if (!other.Empty()) {
        SetData(other.data_.get(), other.size_);
    }
}

SensitiveValue::~SensitiveValue()
{
    Clear();
}

SensitiveValue &SensitiveValue::operator=(const SensitiveValue &other)
{
    Clear();
    if (!other.Empty()) {
        SetData(other.data_.get(), other.size_);
    }
    return *this;
}

SensitiveValue &SensitiveValue::operator=(SensitiveValue &&other) noexcept
{
    Clear();
    data_ = std::move(other.data_);
    size_ = other.size_;
    other.size_ = 0;
    return *this;
}

SensitiveValue &SensitiveValue::operator=(const char *str)
{
    Clear();
    SetData(str, std::strlen(str));
    return *this;
}

SensitiveValue &SensitiveValue::operator=(const std::string &str)
{
    Clear();
    SetData(str.data(), str.length());
    return *this;
}

bool SensitiveValue::Empty() const
{
    return data_ == nullptr || size_ == 0;
}

const char *SensitiveValue::GetData() const
{
    return Empty() ? "" : data_.get();
}

size_t SensitiveValue::GetSize() const
{
    return size_;
}

void SensitiveValue::SetData(const char *str, size_t size)
{
    if (str != nullptr && size > 0) {
        try {
            data_ = std::make_unique<char[]>(size + 1);
        } catch (const std::bad_alloc &e) {
            LOG(WARNING) << "alloc failed";
            return;
        }
        size_ = size;
        int ret = memcpy_s(data_.get(), size_, str, size_);
        if (ret != EOK) {
            LOG(WARNING) << FormatString("memcpy failed, ret = %d", ret);
        }
    }
}
void SensitiveValue::Clear()
{
    if (data_ != nullptr && size_ > 0) {
        int ret = memset_s(data_.get(), size_, 0, size_);
        if (ret != EOK) {
            LOG(WARNING) << FormatString("memset failed, ret = %d", ret);
        }
    }
    size_ = 0;
    data_ = nullptr;
}

bool SensitiveValue::MoveTo(std::unique_ptr<char[]> &outData, size_t &outSize)
{
    if (Empty()) {
        return false;
    }
    outData = std::move(data_);
    outSize = size_;
    size_ = 0;
    return true;
}

bool SensitiveValue::operator==(const SensitiveValue &other) const
{
    if (size_ != other.size_) {
        return false;
    }

    if (size_ == 0) {
        return true;
    }

    if (data_ == nullptr || other.data_ == nullptr) {
        return false;
    }

    return memcmp(data_.get(), other.data_.get(), size_) == 0;
}
}  // namespace datasystem