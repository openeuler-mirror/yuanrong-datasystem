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

using RdmaHandle = void *;
using SocketHandle = void *;
using FdHandle = void *;

/* Unused parameter declaration */
constexpr uint32_t INVALID_VALUE_RANKID = 0xFFFFFFFF;    // rank id非法值
constexpr uint32_t INVALID_VALUE_RANKSIZE = 0xFFFFFFFF;  // rank size非法值
constexpr uint32_t INVALID_UINT = 0xFFFFFFFF;
constexpr uint64_t INVALID_U64 = 0xFFFFFFFFFFFFFFFF;
constexpr int32_t INVALID_INT = 0xFFFFFFFF;
constexpr int64_t INVALID_S64 = 0xFFFFFFFFFFFFFFFF;
constexpr int32_t INVALID_VALUE_STAGE = -1;
constexpr uint32_t INVALID_IPV4_ADDR = 0;
constexpr uint32_t INVALID_QOSCFG = 0xFFFFFFFF;

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

#define unlikely(x) x

#define LOG_PRINT(module, logType, szFormat, ...)            \
    do {                                                     \
        if (unlikely(CheckLogLevel(module, logType) == 1)) { \
            fprintf(stderr, "error\n");                      \
        }                                                    \
    } while (0)

#define MODULE_ERROR(format, ...)   \
    do {                            \
        fprintf(stderr, "error\n"); \
    } while (0)

#define HCCL_ERROR(...) MODULE_ERROR(__VA_ARGS__)

/* 检查指针, 若指针为NULL, 则记录日志, 并返回错误 Returns HCCL_E_PTR  */
#define CHK_PTR_NULL(ptr)                                                 \
    do {                                                                  \
        if (unlikely((ptr) == nullptr)) {                                 \
            fprintf(stderr, "ptr [%s] is NULL, return HCCL_E_PTR", #ptr); \
            return HCCL_E_PTR;                                            \
        }                                                                 \
    } while (0)

/* 检查函数返回值, 并返回指定错误码 */
#define CHK_RET(call)                                                  \
    do {                                                               \
        HcclResult hcclRet = call;                                     \
        if (unlikely(hcclRet != HCCL_SUCCESS)) {                       \
            if (hcclRet == HCCL_E_AGAIN) {                             \
                printf("call trace: hcclRet -> %d", hcclRet);          \
            } else {                                                   \
                fprintf(stderr, "call trace: hcclRet -> %d", hcclRet); \
            }                                                          \
            return hcclRet;                                            \
        }                                                              \
    } while (0)

#endif  // P2P_COMM_H
