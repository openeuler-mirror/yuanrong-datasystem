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
#include "tools/hccl-convert.h"

size_t HcclDataTypeSizes[HCCL_NUM_DATATYPES] = {
    1,  // HCCL_DATA_TYPE_INT8 = 0,    /**< int8 */
    2,  // HCCL_DATA_TYPE_INT16 = 1,   /**< int16 */
    4,  // HCCL_DATA_TYPE_INT32 = 2,   /**< int32 */
    2,  // HCCL_DATA_TYPE_FP16 = 3,    /**< fp16 */
    4,  // HCCL_DATA_TYPE_FP32 = 4,    /**< fp32 */
    8,  // HCCL_DATA_TYPE_INT64 = 5,    /**< int64 */
    8,  // HCCL_DATA_TYPE_UINT64 = 6,    /**< uint64 */
    1,  // HCCL_DATA_TYPE_UINT8 = 7,    /**< uint8 */
    2,  // HCCL_DATA_TYPE_UINT16 = 8,   /**< uint16 */
    4,  // HCCL_DATA_TYPE_UINT32 = 9,   /**< uint32 */
    8,  // HCCL_DATA_TYPE_FP64 = 10, /**< fp64 */
    8,  // HCCL_DATA_TYPE_BFP16 = 11,    /**< bfp16 */
    16  // HCCL_DATA_TYPE_INT128 = 12,   /**< int128 */
};

size_t GetHcclDataSizeBytes(HcclDataType dataType)
{
    if (dataType > HCCL_NUM_DATATYPES) {
        return 0;
    }

    return HcclDataTypeSizes[dataType];
}
