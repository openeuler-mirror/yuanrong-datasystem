/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "datasystem/common/rdma/npu/hixl_plugin_api.h"

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace {

enum FakeMode : uint32_t {
    VALID = 0,
    REJECT_ABI = 1,
    BAD_ABI_VERSION = 2,
    SHORT_API_TABLE = 3,
    NULL_REQUIRED_FUNCTION = 4,
};

std::atomic<uint32_t> g_mode{ VALID };
std::atomic<uint32_t> g_getApiCalls{ 0 };

DsHixlResult CreateEngine(DsHixlEngineHandle *engine)
{
    if (engine == nullptr) {
        return DS_HIXL_INVALID_ARGUMENT;
    }
    *engine = reinterpret_cast<DsHixlEngineHandle>(static_cast<uintptr_t>(1));
    return DS_HIXL_OK;
}

DsHixlResult EngineOnly(DsHixlEngineHandle)
{
    return DS_HIXL_OK;
}

DsHixlResult InitializeEngine(DsHixlEngineHandle, DsHixlStringView, const DsHixlOption *, uint32_t, uint32_t *)
{
    return DS_HIXL_OK;
}

DsHixlResult EndpointOperation(DsHixlEngineHandle, DsHixlStringView, int32_t, uint32_t *)
{
    return DS_HIXL_OK;
}

DsHixlResult RegisterMemory(DsHixlEngineHandle, const DsHixlRegisterMemoryRequest *, DsHixlMemHandle *handle,
                            uint32_t *)
{
    if (handle == nullptr) {
        return DS_HIXL_INVALID_ARGUMENT;
    }
    *handle = reinterpret_cast<DsHixlMemHandle>(static_cast<uintptr_t>(1));
    return DS_HIXL_OK;
}

DsHixlResult DeregisterMemory(DsHixlEngineHandle, DsHixlMemHandle, uint32_t *)
{
    return DS_HIXL_OK;
}

DsHixlResult TransferSync(DsHixlEngineHandle, const DsHixlTransferRequest *, uint32_t *)
{
    return DS_HIXL_OK;
}

const DsHixlApi VALID_API = {
    DS_HIXL_ABI_VERSION_1,
    sizeof(DsHixlApi),
    CreateEngine,
    EngineOnly,
    EngineOnly,
    InitializeEngine,
    EndpointOperation,
    EndpointOperation,
    RegisterMemory,
    DeregisterMemory,
    TransferSync,
};

DsHixlApi MakeApi(uint32_t mode)
{
    DsHixlApi api = VALID_API;
    if (mode == BAD_ABI_VERSION) {
        api.abiVersion = DS_HIXL_ABI_VERSION_1 + 1;
    } else if (mode == SHORT_API_TABLE) {
        api.structSize = offsetof(DsHixlApi, transfer_sync);
    } else if (mode == NULL_REQUIRED_FUNCTION) {
        api.transfer_sync = nullptr;
    }
    return api;
}

DsHixlApi g_api = VALID_API;

}  // namespace

extern "C" DS_HIXL_EXPORT void FakeHixlSetMode(uint32_t mode)
{
    g_mode.store(mode, std::memory_order_relaxed);
}

extern "C" DS_HIXL_EXPORT void FakeHixlReset()
{
    g_mode.store(VALID, std::memory_order_relaxed);
    g_getApiCalls.store(0, std::memory_order_relaxed);
}

extern "C" DS_HIXL_EXPORT uint32_t FakeHixlGetApiCallCount()
{
    return g_getApiCalls.load(std::memory_order_relaxed);
}

extern "C" DS_HIXL_EXPORT DsHixlResult DsHixlGetApi(uint32_t requestedAbiVersion, const DsHixlApi **api)
{
    g_getApiCalls.fetch_add(1, std::memory_order_relaxed);
    if (api == nullptr) {
        return DS_HIXL_INVALID_ARGUMENT;
    }
    *api = nullptr;
    const uint32_t mode = g_mode.load(std::memory_order_relaxed);
    if (requestedAbiVersion != DS_HIXL_ABI_VERSION_1 || mode == REJECT_ABI) {
        return DS_HIXL_NOT_SUPPORTED;
    }
    g_api = MakeApi(mode);
    *api = &g_api;
    return DS_HIXL_OK;
}
