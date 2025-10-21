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
 * Description: Defines the object enum value.
 */
#ifndef DATASYSTEM_OBJECT_ENUM_H
#define DATASYSTEM_OBJECT_ENUM_H

#include <cstdint>
#include <memory>

#include "datasystem/utils/status.h"

namespace datasystem {

enum class WriteMode : int {
    NONE_L2_CACHE = 0,
    WRITE_THROUGH_L2_CACHE = 1,  // sync write
    WRITE_BACK_L2_CACHE = 2,     // async write
    NONE_L2_CACHE_EVICT =  3,    // evictable write
};

enum class ConsistencyType : int {
    PRAM = 0,
    CAUSAL = 1,
};

enum class DataType : uint8_t {
    DATA_TYPE_INT8 = 0,   /**< int8 */
    DATA_TYPE_INT16 = 1,  /**< int16 */
    DATA_TYPE_INT32 = 2,  /**< int32 */
    DATA_TYPE_FP16 = 3,   /**< fp16 */
    DATA_TYPE_FP32 = 4,   /**< fp32 */
    DATA_TYPE_INT64 = 5,  /**< int64 */
    DATA_TYPE_UINT64 = 6, /**< uint64 */
    DATA_TYPE_UINT8 = 7,  /**< uint8 */
    DATA_TYPE_UINT16 = 8, /**< uint16 */
    DATA_TYPE_UINT32 = 9, /**< uint32 */
    DATA_TYPE_FP64 = 10,  /**< fp64 */
    DATA_TYPE_BFP16 = 11, /**< bfp16 */
    DATA_TYPE_RESERVED    /**< reserved */
};

enum class LifetimeType : uint8_t {
    REFERENCE = 0,
    MOVE = 1,
};

enum class CacheType : int {
    MEMORY = 0,
    DISK = 1,
};

enum class ServerState : uint8_t {
    NORMAL = 0,
    REBOOT = 1,
};
}  // namespace datasystem
#endif  // DATASYSTEM_OBJECT_ENUM_H
