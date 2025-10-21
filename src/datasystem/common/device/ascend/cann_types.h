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
 * Description: The AscendCL device context plugin.
 */

#ifndef DATASYSTEM_COMMON_DEVICE_CANN_TYPES_H
#define DATASYSTEM_COMMON_DEVICE_CANN_TYPES_H

#ifdef BUILD_HETERO
#include <acl/acl.h>
#include <hccl/hccl.h>
#include <runtime/dev.h>
#include <runtime/event.h>
#include <runtime/rt_ffts_plus.h>
#include <runtime/rt_ffts_plus_define.h>
#include <runtime/rt_stars.h>
#include <runtime/rt_stars_define.h>
#else
#include <stdint.h>

namespace datasystem {
#ifdef __cplusplus
extern "C" {
#endif

typedef void *aclrtStream;
typedef void *rtNotify_t;
typedef void (*aclrtCallback)(void *userData);

/**
 * @brief handle to HCCL communicator
 */
typedef void *HcclComm;

/**
 * @brief HCCL data type
 */
typedef enum {
    HCCL_DATA_TYPE_INT8 = 0,   /**< int8 */
    HCCL_DATA_TYPE_INT16 = 1,  /**< int16 */
    HCCL_DATA_TYPE_INT32 = 2,  /**< int32 */
    HCCL_DATA_TYPE_FP16 = 3,   /**< fp16 */
    HCCL_DATA_TYPE_FP32 = 4,   /**< fp32 */
    HCCL_DATA_TYPE_INT64 = 5,  /**< int64 */
    HCCL_DATA_TYPE_UINT64 = 6, /**< uint64 */
    HCCL_DATA_TYPE_UINT8 = 7,  /**< uint8 */
    HCCL_DATA_TYPE_UINT16 = 8, /**< uint16 */
    HCCL_DATA_TYPE_UINT32 = 9, /**< uint32 */
    HCCL_DATA_TYPE_FP64 = 10,  /**< fp64 */
    HCCL_DATA_TYPE_BFP16 = 11, /**< bfp16 */
    HCCL_DATA_TYPE_RESERVED    /**< reserved */
} HcclDataType;

/**
 * @brief HCCL functions return value definition
 */
typedef enum {
    HCCL_SUCCESS = 0,              /**< success */
    HCCL_E_PARA = 1,               /**< parameter error */
    HCCL_E_PTR = 2,                /**< empty pointer */
    HCCL_E_MEMORY = 3,             /**< memory error */
    HCCL_E_INTERNAL = 4,           /**< internal error */
    HCCL_E_NOT_SUPPORT = 5,        /**< not support feature */
    HCCL_E_NOT_FOUND = 6,          /**< not found specific resource */
    HCCL_E_UNAVAIL = 7,            /**< resource unavailable */
    HCCL_E_SYSCALL = 8,            /**< call system interface error */
    HCCL_E_TIMEOUT = 9,            /**< timeout */
    HCCL_E_OPEN_FILE_FAILURE = 10, /**< open file fail */
    HCCL_E_TCP_CONNECT = 11,       /**< tcp connect fail */
    HCCL_E_ROCE_CONNECT = 12,      /**< roce connect fail */
    HCCL_E_TCP_TRANSFER = 13,      /**< tcp transfer fail */
    HCCL_E_ROCE_TRANSFER = 14,     /**< roce transfer fail */
    HCCL_E_RUNTIME = 15,           /**< call runtime api fail */
    HCCL_E_DRV = 16,               /**< call driver api fail */
    HCCL_E_PROFILING = 17,         /**< call profiling api fail */
    HCCL_E_CCE = 18,               /**< call cce api fail */
    HCCL_E_NETWORK = 19,           /**< call network api fail */
    HCCL_E_AGAIN = 20,             /**< try again */
    HCCL_E_REMOTE = 21,            /**< error cqe */
    HCCL_E_RESERVED                /**< reserved */
} HcclResult;

typedef enum aclrtMemcpyKind {
    ACL_MEMCPY_HOST_TO_HOST,
    ACL_MEMCPY_HOST_TO_DEVICE,
    ACL_MEMCPY_DEVICE_TO_HOST,
    ACL_MEMCPY_DEVICE_TO_DEVICE,
} aclrtMemcpyKind;

typedef enum aclrtMemMallocPolicy {
    ACL_MEM_MALLOC_HUGE_FIRST,
    ACL_MEM_MALLOC_HUGE_ONLY,
    ACL_MEM_MALLOC_NORMAL_ONLY,
    ACL_MEM_MALLOC_HUGE_FIRST_P2P,
    ACL_MEM_MALLOC_HUGE_ONLY_P2P,
    ACL_MEM_MALLOC_NORMAL_ONLY_P2P,
    ACL_MEM_TYPE_LOW_BAND_WIDTH = 0x0100,
    ACL_MEM_TYPE_HIGH_BAND_WIDTH = 0x1000,
} aclrtMemMallocPolicy;

const uint32_t HCCL_ROOT_INFO_BYTES = 4108;  // 4108: root info length
/**
 * @brief HCCL root info
 */
typedef struct HcclRootInfoDef {
    char internal[HCCL_ROOT_INFO_BYTES];
} HcclRootInfo;

/**
 * @brief Event used for synchronization in the stream
 */
typedef void *aclrtEvent;
/*
 * @brief The acl event recorded status.
 */
typedef enum aclrtEventRecordedStatus {
    // The event is not recorded in the stream, or the event recorded in the stream
    // is not executed or fails to be executed.
    ACL_EVENT_RECORDED_STATUS_NOT_READY = 0,
    // The event recorded in the stream is successfully executed.
    ACL_EVENT_RECORDED_STATUS_COMPLETE = 1,
} aclrtEventRecordedStatus;

typedef enum aclrtCallbackBlockType {
    ACL_CALLBACK_NO_BLOCK,
    ACL_CALLBACK_BLOCK,
} aclrtCallbackBlockType;

}  // namespace datasystem
#ifdef __cplusplus
};
#endif
#endif
#endif
