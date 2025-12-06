/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2022. All rights reserved.
 * Description: 公共常量及宏
 * Author: zhenghuibin
 * Create: 2019-11-28
 */

#ifndef P2P_COMM_H
#define P2P_COMM_H

#include <stdint.h>
#include "hccl/hccl.h"
#include "hccl/hccl_common.h"

using RdmaHandle = void *;
using SocketHandle = void *;
using FdHandle = void *;

enum class HcclCMDType {
    HCCL_CMD_INVALID = 0,
    HCCL_CMD_BROADCAST = 1,
    HCCL_CMD_ALLREDUCE,
    HCCL_CMD_REDUCE,
    HCCL_CMD_SEND,
    HCCL_CMD_RECEIVE,
    HCCL_CMD_ALLGATHER,
    HCCL_CMD_REDUCE_SCATTER,
    HCCL_CMD_ALLTOALLV,
    HCCL_CMD_ALLTOALLVC,
    HCCL_CMD_GATHER,
    HCCL_CMD_MAX
};

#define unlikely(x)     x

#define LOG_PRINT(module, logType, szFormat, ...) do {                        \
    if (unlikely(CheckLogLevel(module, logType) == 1)) { \
        fprintf(stderr, "error\n"); \
    } \
} while (0)

#define MODULE_ERROR(format, ...) do { \
    fprintf(stderr, "error\n"); \
} while (0)

#endif // P2P_COMM_H
