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
#include <unordered_map>
#include "datasystem/common/device/device_manager_base.h"
#else
#include <stdint.h>
#include <unordered_map>
#include "datasystem/common/device/device_manager_base.h"

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
    HCCL_E_SUSPENDING = 22,        /**< error communicator suspending */
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

typedef enum aclrtMemLocationType {
    ACL_MEM_LOCATION_TYPE_HOST = 0,
    ACL_MEM_LOCATION_TYPE_DEVICE,
} aclrtMemLocationType;

typedef struct aclrtMemLocation {
    uint32_t id;
    aclrtMemLocationType type;
} aclrtMemLocation;

typedef struct {
    aclrtMemLocation dstLoc;
    aclrtMemLocation srcLoc;
    uint8_t rsv[16];
} aclrtMemcpyBatchAttr;

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

#ifdef __cplusplus
};
#endif
}  // namespace datasystem
#endif

// ==================== Type Conversion Utilities ====================

namespace datasystem {

/**
 * @brief Convert CommDataType to HcclDataType using static map
 * @param[in] dataType Source CommDataType value
 * @param[out] result Reference to store the converted HcclDataType
 * @return Status::OK() if conversion succeeded, K_NOT_FOUND if type not in map
 */
inline Status ToHcclDataType(CommDataType dataType, HcclDataType &result)
{
    static const std::unordered_map<int, HcclDataType> mapping = {
        {static_cast<int>(CommDataType::INT8), HCCL_DATA_TYPE_INT8},
        {static_cast<int>(CommDataType::INT16), HCCL_DATA_TYPE_INT16},
        {static_cast<int>(CommDataType::INT32), HCCL_DATA_TYPE_INT32},
        {static_cast<int>(CommDataType::INT64), HCCL_DATA_TYPE_INT64},
        {static_cast<int>(CommDataType::UINT8), HCCL_DATA_TYPE_UINT8},
        {static_cast<int>(CommDataType::UINT16), HCCL_DATA_TYPE_UINT16},
        {static_cast<int>(CommDataType::UINT32), HCCL_DATA_TYPE_UINT32},
        {static_cast<int>(CommDataType::UINT64), HCCL_DATA_TYPE_UINT64},
        {static_cast<int>(CommDataType::FLOAT16), HCCL_DATA_TYPE_FP16},
        {static_cast<int>(CommDataType::FLOAT32), HCCL_DATA_TYPE_FP32},
        {static_cast<int>(CommDataType::FLOAT64), HCCL_DATA_TYPE_FP64},
        {static_cast<int>(CommDataType::BFLOAT16), HCCL_DATA_TYPE_BFP16},
    };
    
    auto it = mapping.find(static_cast<int>(dataType));
    if (it == mapping.end()) {
        return Status(StatusCode::K_NOT_SUPPORTED,
            "CommDataType not supported for HCCL conversion");
    }
    result = it->second;
    return Status::OK();
}

/**
 * @brief Convert HcclDataType to CommDataType using static map
 * @param[in] dataType Source HcclDataType value
 * @param[out] result Reference to store the converted CommDataType
 * @return Status::OK() if conversion succeeded, K_NOT_FOUND if type not in map
 */
inline Status ToCommDataType(HcclDataType dataType, CommDataType &result)
{
    static const std::unordered_map<int, CommDataType> mapping = {
        {HCCL_DATA_TYPE_INT8, CommDataType::INT8},
        {HCCL_DATA_TYPE_INT16, CommDataType::INT16},
        {HCCL_DATA_TYPE_INT32, CommDataType::INT32},
        {HCCL_DATA_TYPE_INT64, CommDataType::INT64},
        {HCCL_DATA_TYPE_UINT8, CommDataType::UINT8},
        {HCCL_DATA_TYPE_UINT16, CommDataType::UINT16},
        {HCCL_DATA_TYPE_UINT32, CommDataType::UINT32},
        {HCCL_DATA_TYPE_UINT64, CommDataType::UINT64},
        {HCCL_DATA_TYPE_FP16, CommDataType::FLOAT16},
        {HCCL_DATA_TYPE_FP32, CommDataType::FLOAT32},
        {HCCL_DATA_TYPE_FP64, CommDataType::FLOAT64},
        {HCCL_DATA_TYPE_BFP16, CommDataType::BFLOAT16},
    };
    
    auto it = mapping.find(dataType);
    if (it == mapping.end()) {
        return Status(StatusCode::K_NOT_SUPPORTED,
            "HcclDataType not supported for CommDataType conversion");
    }
    result = it->second;
    return Status::OK();
}

/**
 * @brief Convert MemcpyKind to aclrtMemcpyKind using static map
 * @param[in] kind Source MemcpyKind value
 * @param[out] result Reference to store the converted aclrtMemcpyKind
 * @return Status::OK() if conversion succeeded, K_NOT_SUPPORTED if kind not in map
 */
inline Status ToAclMemcpyKind(MemcpyKind kind, aclrtMemcpyKind &result)
{
    static const std::unordered_map<int, aclrtMemcpyKind> mapping = {
        {static_cast<int>(MemcpyKind::HOST_TO_HOST), ACL_MEMCPY_HOST_TO_HOST},
        {static_cast<int>(MemcpyKind::HOST_TO_DEVICE), ACL_MEMCPY_HOST_TO_DEVICE},
        {static_cast<int>(MemcpyKind::DEVICE_TO_HOST), ACL_MEMCPY_DEVICE_TO_HOST},
        {static_cast<int>(MemcpyKind::DEVICE_TO_DEVICE), ACL_MEMCPY_DEVICE_TO_DEVICE},
    };
    
    auto it = mapping.find(static_cast<int>(kind));
    if (it == mapping.end()) {
        return Status(StatusCode::K_NOT_SUPPORTED,
            "MemcpyKind not supported for ACL conversion");
    }
    result = it->second;
    return Status::OK();
}

/**
 * @brief Convert aclrtMemcpyKind to MemcpyKind using static map
 * @param[in] kind Source aclrtMemcpyKind value
 * @param[out] result Reference to store the converted MemcpyKind
 * @return Status::OK() if conversion succeeded, K_NOT_SUPPORTED if kind not in map
 */
inline Status ToMemcpyKind(aclrtMemcpyKind kind, MemcpyKind &result)
{
    static const std::unordered_map<int, MemcpyKind> mapping = {
        {ACL_MEMCPY_HOST_TO_HOST, MemcpyKind::HOST_TO_HOST},
        {ACL_MEMCPY_HOST_TO_DEVICE, MemcpyKind::HOST_TO_DEVICE},
        {ACL_MEMCPY_DEVICE_TO_HOST, MemcpyKind::DEVICE_TO_HOST},
        {ACL_MEMCPY_DEVICE_TO_DEVICE, MemcpyKind::DEVICE_TO_DEVICE},
    };
    
    auto it = mapping.find(kind);
    if (it == mapping.end()) {
        return Status(StatusCode::K_NOT_SUPPORTED,
            "aclrtMemcpyKind not supported for MemcpyKind conversion");
    }
    result = it->second;
    return Status::OK();
}

/**
 * @brief Convert MemMallocPolicy to aclrtMemMallocPolicy using static map
 * @param[in] policy Source MemMallocPolicy value
 * @param[out] result Reference to store the converted aclrtMemMallocPolicy
 * @return Status::OK() if conversion succeeded, K_NOT_SUPPORTED if policy not in map
 */
inline Status ToAclMemMallocPolicy(MemMallocPolicy policy, aclrtMemMallocPolicy &result)
{
    static const std::unordered_map<int, aclrtMemMallocPolicy> mapping = {
        {static_cast<int>(MemMallocPolicy::HUGE_FIRST), ACL_MEM_MALLOC_HUGE_FIRST},
        {static_cast<int>(MemMallocPolicy::HUGE_ONLY), ACL_MEM_MALLOC_HUGE_ONLY},
        {static_cast<int>(MemMallocPolicy::NORMAL_ONLY), ACL_MEM_MALLOC_NORMAL_ONLY},
    };
    
    auto it = mapping.find(static_cast<int>(policy));
    if (it == mapping.end()) {
        return Status(StatusCode::K_NOT_SUPPORTED,
            "MemMallocPolicy not supported for ACL conversion");
    }
    result = it->second;
    return Status::OK();
}

/**
 * @brief Convert CallbackBlockType to aclrtCallbackBlockType using static map
 * @param[in] blockType Source CallbackBlockType value
 * @param[out] result Reference to store the converted aclrtCallbackBlockType
 * @return Status::OK() if conversion succeeded, K_NOT_SUPPORTED if type not in map
 */
inline Status ToAclCallbackBlockType(CallbackBlockType blockType, aclrtCallbackBlockType &result)
{
    static const std::unordered_map<int, aclrtCallbackBlockType> mapping = {
        {static_cast<int>(CallbackBlockType::BLOCK), ACL_CALLBACK_BLOCK},
        {static_cast<int>(CallbackBlockType::NO_BLOCK), ACL_CALLBACK_NO_BLOCK},
    };
    
    auto it = mapping.find(static_cast<int>(blockType));
    if (it == mapping.end()) {
        return Status(StatusCode::K_NOT_SUPPORTED,
            "CallbackBlockType not supported for ACL conversion");
    }
    result = it->second;
    return Status::OK();
}

/**
 * @brief Convert CommRootInfo to HcclRootInfo pointer
 * @note Both structures have compatible memory layout
 * @param[in] rootInfo Source CommRootInfo pointer
 * @param[out] result Reference to store the converted HcclRootInfo pointer
 * @return Status::OK() if conversion succeeded, K_INVALID_PARAMS if rootInfo is null
 */
inline Status ToHcclRootInfo(CommRootInfo *rootInfo, HcclRootInfo *&result)
{
    if (sizeof(CommRootInfo) < sizeof(HcclRootInfo)) {
        return Status(StatusCode::K_INVALID, "CommRootInfo size is smaller than HcclRootInfo");
    }
    result = reinterpret_cast<HcclRootInfo*>(rootInfo);
    return Status::OK();
}

/**
 * @brief Convert const CommRootInfo to const HcclRootInfo pointer
 * @note Both structures have compatible memory layout
 * @param[in] rootInfo Source const CommRootInfo pointer
 * @param[out] result Reference to store the converted const HcclRootInfo pointer
 * @return Status::OK() if conversion succeeded, K_INVALID_PARAMS if rootInfo is null
 */
inline Status ToHcclRootInfo(const CommRootInfo *rootInfo, const HcclRootInfo *&result)
{
    if (sizeof(CommRootInfo) < sizeof(HcclRootInfo)) {
        return Status(StatusCode::K_INVALID, "CommRootInfo size is smaller than HcclRootInfo");
    }
    result = reinterpret_cast<const HcclRootInfo*>(rootInfo);
    return Status::OK();
}

}  // namespace datasystem
#endif // DATASYSTEM_COMMON_DEVICE_CANN_TYPES_H