/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#ifndef DATASYSTEM_COMMON_RDMA_NPU_HIXL_PLUGIN_API_H
#define DATASYSTEM_COMMON_RDMA_NPU_HIXL_PLUGIN_API_H

#include <stddef.h>
#include <stdint.h>

#define DS_HIXL_ABI_VERSION_1 1U
#define DS_HIXL_MAX_OPTION_COUNT 16U
#define DS_HIXL_MAX_STRING_LENGTH 4096U

typedef enum DsHixlResult {
    DS_HIXL_OK = 0,
    DS_HIXL_INVALID_ARGUMENT = 1,
    DS_HIXL_NOT_SUPPORTED = 2,
    DS_HIXL_RUNTIME_ERROR = 3,
} DsHixlResult;

#define DS_HIXL_MEMORY_DEVICE 0U
#define DS_HIXL_MEMORY_HOST 1U

#define DS_HIXL_TRANSFER_READ 0U
#define DS_HIXL_TRANSFER_WRITE 1U

typedef struct DsHixlEngine *DsHixlEngineHandle;
typedef struct DsHixlMemory *DsHixlMemHandle;

typedef struct DsHixlStringView {
    const char *data;
    uint64_t size;
} DsHixlStringView;

typedef struct DsHixlOption {
    DsHixlStringView key;
    DsHixlStringView value;
} DsHixlOption;

typedef struct DsHixlTransferDesc {
    uintptr_t localAddr;
    uintptr_t remoteAddr;
    uint64_t length;
} DsHixlTransferDesc;

typedef struct DsHixlRegisterMemoryRequest {
    uintptr_t address;
    uint64_t length;
    uint32_t memoryType;
} DsHixlRegisterMemoryRequest;

typedef struct DsHixlTransferRequest {
    DsHixlStringView remoteEndpoint;
    uint32_t operation;
    const DsHixlTransferDesc *descriptors;
    uint32_t descriptorCount;
    int32_t timeoutMs;
} DsHixlTransferRequest;

typedef struct DsHixlApi {
    uint32_t abiVersion;
    uint32_t structSize;

    DsHixlResult (*create_engine)(DsHixlEngineHandle *engine);
    DsHixlResult (*finalize_engine)(DsHixlEngineHandle engine);
    DsHixlResult (*destroy_engine)(DsHixlEngineHandle engine);

    DsHixlResult (*initialize_engine)(DsHixlEngineHandle engine, DsHixlStringView localEndpoint,
                                      const DsHixlOption *options, uint32_t optionCount, uint32_t *vendorStatus);
    DsHixlResult (*connect_engine)(DsHixlEngineHandle engine, DsHixlStringView remoteEndpoint, int32_t timeoutMs,
                                   uint32_t *vendorStatus);
    DsHixlResult (*disconnect_engine)(DsHixlEngineHandle engine, DsHixlStringView remoteEndpoint, int32_t timeoutMs,
                                      uint32_t *vendorStatus);
    DsHixlResult (*register_memory)(DsHixlEngineHandle engine, const DsHixlRegisterMemoryRequest *request,
                                    DsHixlMemHandle *memoryHandle, uint32_t *vendorStatus);
    DsHixlResult (*deregister_memory)(DsHixlEngineHandle engine, DsHixlMemHandle memoryHandle,
                                      uint32_t *vendorStatus);
    DsHixlResult (*transfer_sync)(DsHixlEngineHandle engine, const DsHixlTransferRequest *request,
                                  uint32_t *vendorStatus);
} DsHixlApi;

#if defined(__GNUC__)
#define DS_HIXL_EXPORT __attribute__((visibility("default")))
#else
#define DS_HIXL_EXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

DS_HIXL_EXPORT DsHixlResult DsHixlGetApi(uint32_t requestedAbiVersion, const DsHixlApi **api);
typedef DsHixlResult (*DsHixlGetApiFunc)(uint32_t requestedAbiVersion, const DsHixlApi **api);

#ifdef __cplusplus
}
#endif

#endif  // DATASYSTEM_COMMON_RDMA_NPU_HIXL_PLUGIN_API_H
