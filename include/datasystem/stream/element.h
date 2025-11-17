/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Define api of stream cache.
 */
#ifndef DATASYSTEM_STREAM_CACHE_ELEMENT_H
#define DATASYSTEM_STREAM_CACHE_ELEMENT_H

#include <climits>
#include <cstring>
#include <malloc.h>
#include <memory>

#include "datasystem/utils/status.h"

namespace datasystem {
/**
 * @brief Element struct settings.
 */
struct Element {
    Element(uint8_t *ptr = nullptr, uint64_t size = 0, uint64_t id = ULONG_MAX) : ptr(ptr), size(size), id(id)
    {
    }

    ~Element() = default;

    /**
     * @brief The pointer of element.
     */
    uint8_t *ptr;

    /**
     * @brief The size of element.
     */
    uint64_t size;

    /**
     * @brief The id of element which can created and increased by datasystem automatically.
     */
    uint64_t id;
};
}  // namespace datasystem
#endif  // DATASYSTEM_STREAM_CACHE_ELEMENT_H
