/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
#ifndef DATASYSTEM_OBJECT_CACHE_DEVICE_DEVICUTIL_H
#define DATASYSTEM_OBJECT_CACHE_DEVICE_DEVICUTIL_H

#include <cstdint>
#include <vector>

#include "datasystem/object_cache/object_enum.h"

namespace datasystem {
/**
 * @brief Gets the bytes from data type.
 * @param[in] dataType The data type of device memory.
 * @return The bytes of data type.
 */
static int GetBytesFromDataType(DataType dataType)
{
    auto twoBytes = 2;
    auto fourBytes = 4;
    auto eightBytes = 8;
    switch (dataType) {
        case DataType::DATA_TYPE_INT8:
        case DataType::DATA_TYPE_UINT8:
            return 1;
        case DataType::DATA_TYPE_INT16:
        case DataType::DATA_TYPE_FP16:
        case DataType::DATA_TYPE_UINT16:
        case DataType::DATA_TYPE_BFP16:
            return twoBytes;
        case DataType::DATA_TYPE_INT32:
        case DataType::DATA_TYPE_FP32:
        case DataType::DATA_TYPE_UINT32:
            return fourBytes;
        case DataType::DATA_TYPE_INT64:
        case DataType::DATA_TYPE_UINT64:
        case DataType::DATA_TYPE_FP64:
            return eightBytes;
        case DataType::DATA_TYPE_RESERVED:
            return 0;
    }
    return 0;
}

struct DataInfo {
    void *devPtr = 0;
    DataType dataType = DataType::DATA_TYPE_INT8;
    uint64_t count = 0;
    uint64_t size = 0;
    int32_t deviceIdx = -1;

    /**
     * @brief Gets the size of data info.
     * @return The size (bytes) of data info.
     */
    size_t Size() const
    {
        return count * GetBytesFromDataType(dataType);
    }
};
}  // namespace datasystem
#endif  // DATASYSTEM_OBJECT_CACHE_DEVICE_DEVICUTIL_H
