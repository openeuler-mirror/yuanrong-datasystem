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
 * Description: The CUDA device context plugin.
 */

#ifndef DATASYSTEM_COMMON_DEVICE_CUDA_TYPES_H
#define DATASYSTEM_COMMON_DEVICE_CUDA_TYPES_H

#if defined(USE_GPU)
#include <cuda.h>
#include <cuda_runtime.h>
#include <nccl.h>
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

typedef void* cudaStream_t;

/**
 * @brief handle to NCCL communicator
 */
typedef void *ncclComm_t;

/**
 * @brief NCCL data type
 */
typedef enum {
    ncclInt8 = 0,   /**< Signed 8-bits integer */
    ncclChar = 1,  /**< Signed 8-bits integer */
    ncclUint8 = 2,  /**< Unsigned 8-bits integer */
    ncclInt32 = 3,   /**< Signed 32-bits integer */
    ncclInt = 4,   /**< Signed 32-bits integer */
    ncclUint32 = 5,  /**< Unsigned 32-bits integer */
    ncclInt64 = 6, /**< Signed 64-bits integer */
    ncclUint64 = 7,  /**< Unsigned 64-bits integer */
    ncclFloat16 = 8, /**< 16-bits floating point */
    ncclHalf = 9, /**< 16-bits floating point */
    ncclFloat32 = 10,  /**< 32-bits floating point */
    ncclFloat = 11, /**< 32-bits floating point */
    ncclFloat64 = 12,  /**< 64-bits floating point */
    ncclDouble = 13,  /**< 64-bits floating point */
    ncclBfloat16 = 14,  /**< 16-bits floating point */
    NCCL_DATA_TYPE_RESERVED    /**< reserved */
} ncclDataType_t;

/**
 * @brief NCCL functions return value definition
 */
typedef enum {
    ncclSuccess = 0,              /**< Function succeeded */
    ncclUnhandledCudaError = 1,   /**< A call to a CUDA function failed */
    ncclSystemError = 2,          /**< A call to the system failed */
    ncclInternalError = 3,        /**< An internal check failed */
    ncclInvalidArgument = 4,      /**< An argument has an invalid value */
    ncclInvalidUsage = 5,         /**< The call to NCCL is incorrect */
    ncclRemoteError = 6,          /**< A call failed possibly due to a network error 
                                    or a remote process exiting prematurely */
    ncclInProgress = 7,           /**< A NCCL operation on the communicator is being enqueued 
                                    and is being progressed in the background */
    NCCL_E_RESERVED                /**< reserved */
} ncclResult_t;

typedef enum cudaMemcpyKind {
    cudaMemcpyHostToHost = 0,
    cudaMemcpyHostToDevice = 1,
    cudaMemcpyDeviceToHost = 2,
    cudaMemcpyDeviceToDevice = 3,
} cudaMemcpyKind;

const uint32_t NCCL_UNIQUE_ID_BYTES = 128;
/**
 * @brief NCCL root info
 */
typedef struct ncclUniqueIdDef {
    char internal[NCCL_UNIQUE_ID_BYTES];
} ncclUniqueId;

/**
 * @brief Event used for synchronization in the stream
 */
typedef void *cudaEvent_t;


#ifdef __cplusplus
};
#endif
}  // namespace datasystem
#endif

// ==================== Type Conversion Utilities ====================

namespace datasystem {

/**
 * @brief Convert CommDataType to NcclDataType using static map
 * @param[in] dataType Source CommDataType value
 * @param[out] result Reference to store the converted NcclDataType
 * @return Status::OK() if conversion succeeded, K_NOT_FOUND if type not in map
 */
inline Status ToNcclDataType(CommDataType dataType, ncclDataType_t &result)
{
    static const std::unordered_map<int, ncclDataType_t> mapping = {
        {static_cast<int>(CommDataType::INT8), ncclInt8},
        {static_cast<int>(CommDataType::INT32), ncclInt32},
        {static_cast<int>(CommDataType::INT64), ncclInt64},
        {static_cast<int>(CommDataType::UINT8), ncclUint8},
        {static_cast<int>(CommDataType::UINT32), ncclUint32},
        {static_cast<int>(CommDataType::UINT64), ncclUint64},
        {static_cast<int>(CommDataType::FLOAT16), ncclFloat16},
        {static_cast<int>(CommDataType::FLOAT32), ncclFloat32},
        {static_cast<int>(CommDataType::FLOAT64), ncclFloat64},
        {static_cast<int>(CommDataType::BFLOAT16), ncclBfloat16},
        {static_cast<int>(CommDataType::INT16), ncclInt8},
        {static_cast<int>(CommDataType::UINT16), ncclInt8},
    };
    
    auto it = mapping.find(static_cast<int>(dataType));
    if (it == mapping.end()) {
        return Status(StatusCode::K_NOT_SUPPORTED,
            "CommDataType not supported for NCCL conversion");
    }
    result = it->second;
    return Status::OK();
}

/**
 * @brief Convert NcclDataType to CommDataType using static map
 * @param[in] dataType Source NcclDataType value
 * @param[out] result Reference to store the converted CommDataType
 * @return Status::OK() if conversion succeeded, K_NOT_FOUND if type not in map
 */
inline Status ToCommDataType(ncclDataType_t dataType, CommDataType &result)
{
    static const std::unordered_map<int, CommDataType> mapping = {
        {ncclInt8, CommDataType::INT8},
        {ncclInt32, CommDataType::INT32},
        {ncclInt64, CommDataType::INT64},
        {ncclUint8, CommDataType::UINT8},
        {ncclUint32, CommDataType::UINT32},
        {ncclUint64, CommDataType::UINT64},
        {ncclFloat16, CommDataType::FLOAT16},
        {ncclFloat32, CommDataType::FLOAT32},
        {ncclFloat64, CommDataType::FLOAT64},
        {ncclBfloat16, CommDataType::BFLOAT16},
    };
    
    auto it = mapping.find(dataType);
    if (it == mapping.end()) {
        return Status(StatusCode::K_NOT_SUPPORTED,
            "NcclDataType not supported for CommDataType conversion");
    }
    result = it->second;
    return Status::OK();
}

/**
 * @brief Convert MemcpyKind to cudaMemcpyKind using static map
 * @param[in] kind Source MemcpyKind value
 * @param[out] result Reference to store the converted cudaMemcpyKind
 * @return Status::OK() if conversion succeeded, K_NOT_SUPPORTED if kind not in map
 */
inline Status ToCudaMemcpyKind(MemcpyKind kind, cudaMemcpyKind &result)
{
    static const std::unordered_map<int, cudaMemcpyKind> mapping = {
        {static_cast<int>(MemcpyKind::HOST_TO_HOST), cudaMemcpyHostToHost},
        {static_cast<int>(MemcpyKind::HOST_TO_DEVICE), cudaMemcpyHostToDevice},
        {static_cast<int>(MemcpyKind::DEVICE_TO_HOST), cudaMemcpyDeviceToHost},
        {static_cast<int>(MemcpyKind::DEVICE_TO_DEVICE), cudaMemcpyDeviceToDevice},
    };
    
    auto it = mapping.find(static_cast<int>(kind));
    if (it == mapping.end()) {
        return Status(StatusCode::K_NOT_SUPPORTED,
            "MemcpyKind not supported for CUDA conversion");
    }
    result = it->second;
    return Status::OK();
}

/**
 * @brief Convert cudaMemcpyKind to MemcpyKind using static map
 * @param[in] kind Source cudaMemcpyKind value
 * @param[out] result Reference to store the converted MemcpyKind
 * @return Status::OK() if conversion succeeded, K_NOT_SUPPORTED if kind not in map
 */
inline Status ToMemcpyKind(cudaMemcpyKind kind, MemcpyKind &result)
{
    static const std::unordered_map<int, MemcpyKind> mapping = {
        {cudaMemcpyHostToHost, MemcpyKind::HOST_TO_HOST},
        {cudaMemcpyHostToDevice, MemcpyKind::HOST_TO_DEVICE},
        {cudaMemcpyDeviceToHost, MemcpyKind::DEVICE_TO_HOST},
        {cudaMemcpyDeviceToDevice, MemcpyKind::DEVICE_TO_DEVICE},
    };
    
    auto it = mapping.find(kind);
    if (it == mapping.end()) {
        return Status(StatusCode::K_NOT_SUPPORTED,
            "cudaMemcpyKind not supported for MemcpyKind conversion");
    }
    result = it->second;
    return Status::OK();
}


/**
 * @brief Convert CommRootInfo to ncclUniqueId pointer
 * @note Both structures have compatible memory layout
 * @param[in] rootInfo Source CommRootInfo pointer
 * @param[out] result Reference to store the converted ncclUniqueId pointer
 * @return Status::OK() if conversion succeeded, K_INVALID_PARAMS if rootInfo is null
 */
inline Status ToNcclRootInfo(CommRootInfo *rootInfo, ncclUniqueId *&result)
{
    if (sizeof(CommRootInfo) < sizeof(ncclUniqueId)) {
        return Status(StatusCode::K_INVALID, "CommRootInfo size is smaller than ncclUniqueId");
    }
    result = reinterpret_cast<ncclUniqueId*>(rootInfo);
    return Status::OK();
}

/**
 * @brief Convert const CommRootInfo to const ncclUniqueId pointer
 * @note Both structures have compatible memory layout
 * @param[in] rootInfo Source const CommRootInfo pointer
 * @param[out] result Reference to store the converted const ncclUniqueId pointer
 * @return Status::OK() if conversion succeeded, K_INVALID_PARAMS if rootInfo is null
 */
inline Status ToNcclRootInfo(const CommRootInfo *rootInfo, const ncclUniqueId *&result)
{
    if (sizeof(CommRootInfo) < sizeof(ncclUniqueId)) {
        return Status(StatusCode::K_INVALID, "CommRootInfo size is smaller than ncclUniqueId");
    }
    result = reinterpret_cast<const ncclUniqueId*>(rootInfo);
    return Status::OK();
}

}  // namespace datasystem
#endif // DATASYSTEM_COMMON_DEVICE_CUDA_TYPES_H