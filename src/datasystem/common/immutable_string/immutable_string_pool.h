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
 * Description: Immutable string pool declaration.
 */
#ifndef DATASYSTEM_COMMON_IMMUTABLE_STRING_POOL_H
#define DATASYSTEM_COMMON_IMMUTABLE_STRING_POOL_H

#include <string>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/immutable_string/ref_count_string.h"

namespace datasystem {

using RCStringMap = tbb::concurrent_hash_map<RefCountString, bool>;

class ImmutableStringPool {
public:
    ImmutableStringPool() = default;
    ~ImmutableStringPool();
    ImmutableStringPool(ImmutableStringPool &&) = delete;                  // Move construct
    ImmutableStringPool(const ImmutableStringPool &) = delete;             // Copy construct
    ImmutableStringPool &operator=(const ImmutableStringPool &) = delete;  // Copy assign
    ImmutableStringPool &operator=(ImmutableStringPool &&) = delete;       // Move assign

    /**
     * @brief Get the Singleton ImmutableStringPool instance.
     * @return ImmutableStringPool instance.
     */
    static ImmutableStringPool &Instance();

    /**
     * @brief Init the ImmutableStringPool, use it to control the construction and destruction timing.
     */
    void Init();

    /**
     * @brief Intern the std::string to pool and return the handle of this string.
     * @param[in] val The std::string ready to intern.
     * @param[out] The handle of intern string.
     */
    void Intern(const std::string &val, RefCountStringHandle &handle);

    /**
     * @brief Try to Erase the RefCountString by handle if its reference count is 0.
     * @param[in] handle The handle whose ptr_ is ready to erase.
     */
    void Erase(RefCountStringHandle &handle);

    /**
     * @brief Return the size of ImmutableStringPool
     * @return The size of ImmutableStringPool
     */
    size_t Size();

private:
    RCStringMap pool_;
};
}  // namespace datasystem
#endif