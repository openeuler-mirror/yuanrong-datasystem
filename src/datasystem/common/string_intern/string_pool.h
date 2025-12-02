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
 * Description: StringPool Define.
 */
#ifndef DATASYSTEM_COMMON_STRING_INTERN_STRING_POOL_H
#define DATASYSTEM_COMMON_STRING_INTERN_STRING_POOL_H

#include <string>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/string_intern/key_type.h"
#include "datasystem/common/string_intern/string_ptr.h"
#include "datasystem/common/string_intern/string_entity.h"

namespace datasystem {
namespace intern {
using StringEntityMap = tbb::concurrent_hash_map<StringEntity, bool>;

class StringPool {
public:
    ~StringPool();
    StringPool(StringPool &&) = delete;                  // Move construct
    StringPool(const StringPool &) = delete;             // Copy construct
    StringPool &operator=(const StringPool &) = delete;  // Copy assign
    StringPool &operator=(StringPool &&) = delete;       // Move assign

    /**
     * @brief Get the Singleton StringPool instance.
     * @return StringPool instance.
     */
    static StringPool &Instance(KeyType keyType);

    static void InitAll();

    /**
     * @brief Init the StringPool, use it to control the construction and destruction timing.
     */
    void Init() const;

    /**
     * @brief Intern the std::string to pool and return the handle of this string.
     * @param[in] val The std::string ready to intern.
     * @param[out] The handle of intern string.
     */
    StringPtr Intern(const std::string &val);

    /**
     * @brief Try to Erase the StringEntity by handle if its reference count is 0.
     * @param[in] handle The handle whose ptr_ is ready to erase.
     */
    void Erase(StringPtr &handle);

    /**
     * @brief Return the size ofStringPool
     * @return The size ofStringPool
     */
    size_t Size() const;

private:
    StringPool(KeyType keyType) : keyType_(keyType)
    {
    }
    StringEntityMap pool_;
    KeyType keyType_;
};

}  // namespace intern
}  // namespace datasystem
#endif
