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
 * Description: StringPtr implementation.
 */
#ifndef DATASYSTEM_COMMON_STRING_INTERN_STRING_PTR_H
#define DATASYSTEM_COMMON_STRING_INTERN_STRING_PTR_H

#include <cstddef>
#include <string>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/string_intern/string_entity.h"

namespace datasystem {
namespace intern {
/**
 * @brief A handle of StringEntityto control the reference count like shared_ptr.
 */
class StringPtr {
public:
    StringPtr() : ptr_(nullptr)
    {
    }

    explicit StringPtr(const StringEntity &str) : ptr_(&str)
    {
        IncRef();
    }

    /**
     * @brief Get the const reference of std::string.
     * @return The the const reference of std::string.
     */
    const std::string &ToStr() const
    {
        if (ptr_ != nullptr) {
            return ptr_->ToStr();
        }
        static std::string defaultStr;
        return defaultStr;
    }

    /**
     * @brief Get the const reference of StringEntity.
     * @return The the const reference of StringEntity.
     */
    const StringEntity *GetEntity() const
    {
        return ptr_;
    }

    size_t GetHash() const
    {
        static size_t emptyStringHashVal = std::hash<std::string>()("");
        return ptr_ != nullptr ? ptr_->GetHash() : emptyStringHashVal;
    }

    void IncRef() const
    {
        if (ptr_ != nullptr && ptr_->IncRef() == 1) {
            ptr_->IncDelRef();
        }
    }

    bool DecRef() const
    {
        return ptr_ != nullptr ? ptr_->DecRef() : false;
    }

private:
    const StringEntity *ptr_;
};
}  // namespace intern
}  // namespace datasystem

#endif
