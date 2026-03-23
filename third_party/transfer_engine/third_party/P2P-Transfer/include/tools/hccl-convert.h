/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_HCCL_CONVERT_H
#define P2P_HCCL_CONVERT_H
#include <stddef.h>
#include "hccl/hccl.h"

#define HCCL_NUM_DATATYPES 13

// Returns the amount of bytes needed to represent an element of the corresponding data type
size_t GetHcclDataSizeBytes(HcclDataType dataType);

#endif  // P2P_HCCL_CONVERT_H