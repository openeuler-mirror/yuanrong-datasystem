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

#include <hixl/hixl.h>
#include <hixl/hixl_types.h>

#include <exception>
#include <limits>
#include <map>
#include <memory>
#include <new>
#include <utility>
#include <vector>

namespace {

struct HixlEngineContext {
    std::unique_ptr<hixl::Hixl> engine;
    bool initialized = false;
    std::vector<hixl::TransferOpDesc> transferDescriptors;
};

bool IsValidString(DsHixlStringView value, bool allowEmpty)
{
    if (value.size > DS_HIXL_MAX_STRING_LENGTH || (!allowEmpty && value.size == 0)) {
        return false;
    }
    return value.data != nullptr || value.size == 0;
}

hixl::AscendString ToAscendString(DsHixlStringView value)
{
    return value.size == 0 ? hixl::AscendString("")
                           : hixl::AscendString(value.data, static_cast<size_t>(value.size));
}

bool FitsSizeT(uint64_t length)
{
    if constexpr (sizeof(size_t) < sizeof(uint64_t)) {
        return length <= static_cast<uint64_t>(std::numeric_limits<size_t>::max());
    }
    return true;
}

DsHixlResult ToDsResult(hixl::Status status)
{
    if (status == hixl::SUCCESS) {
        return DS_HIXL_OK;
    }
    if (status == hixl::PARAM_INVALID) {
        return DS_HIXL_INVALID_ARGUMENT;
    }
    if (status == hixl::UNSUPPORTED) {
        return DS_HIXL_NOT_SUPPORTED;
    }
    return DS_HIXL_RUNTIME_ERROR;
}

void SetVendorStatus(uint32_t *vendorStatus, hixl::Status status)
{
    if (vendorStatus != nullptr) {
        *vendorStatus = status;
    }
}

template <typename Func>
DsHixlResult InvokeNoexcept(Func &&func) noexcept
{
    try {
        return func();
    } catch (const std::bad_alloc &) {
        return DS_HIXL_RUNTIME_ERROR;
    } catch (const std::exception &) {
        return DS_HIXL_RUNTIME_ERROR;
    } catch (...) {
        return DS_HIXL_RUNTIME_ERROR;
    }
}

HixlEngineContext *GetContext(DsHixlEngineHandle engine)
{
    return reinterpret_cast<HixlEngineContext *>(engine);
}

DsHixlResult CreateEngine(DsHixlEngineHandle *engine) noexcept
{
    return InvokeNoexcept([engine]() {
        if (engine == nullptr) {
            return DS_HIXL_INVALID_ARGUMENT;
        }
        *engine = nullptr;
        auto context = std::make_unique<HixlEngineContext>();
        context->engine = std::make_unique<hixl::Hixl>();
        *engine = reinterpret_cast<DsHixlEngineHandle>(context.release());
        return DS_HIXL_OK;
    });
}

DsHixlResult FinalizeEngine(DsHixlEngineHandle engine) noexcept
{
    return InvokeNoexcept([engine]() {
        auto *context = GetContext(engine);
        if (context == nullptr || context->engine == nullptr) {
            return DS_HIXL_INVALID_ARGUMENT;
        }
        if (context->initialized) {
            context->engine->Finalize();
            context->initialized = false;
        }
        context->transferDescriptors.clear();
        return DS_HIXL_OK;
    });
}

DsHixlResult DestroyEngine(DsHixlEngineHandle engine) noexcept
{
    return InvokeNoexcept([engine]() {
        std::unique_ptr<HixlEngineContext> context(GetContext(engine));
        if (context == nullptr) {
            return DS_HIXL_INVALID_ARGUMENT;
        }
        if (context->engine != nullptr && context->initialized) {
            context->engine->Finalize();
        }
        return DS_HIXL_OK;
    });
}

DsHixlResult InitializeEngine(DsHixlEngineHandle engine, DsHixlStringView localEndpoint,
                              const DsHixlOption *options, uint32_t optionCount, uint32_t *vendorStatus) noexcept
{
    return InvokeNoexcept([=]() {
        auto *context = GetContext(engine);
        SetVendorStatus(vendorStatus, hixl::SUCCESS);
        if (context == nullptr || context->engine == nullptr || context->initialized
            || !IsValidString(localEndpoint, false) || optionCount > DS_HIXL_MAX_OPTION_COUNT
            || (optionCount > 0 && options == nullptr)) {
            return DS_HIXL_INVALID_ARGUMENT;
        }

        std::map<hixl::AscendString, hixl::AscendString> hixlOptions;
        for (uint32_t i = 0; i < optionCount; ++i) {
            if (!IsValidString(options[i].key, false) || !IsValidString(options[i].value, true)) {
                return DS_HIXL_INVALID_ARGUMENT;
            }
            hixlOptions.emplace(ToAscendString(options[i].key), ToAscendString(options[i].value));
        }

        hixl::Status status = context->engine->Initialize(ToAscendString(localEndpoint), hixlOptions);
        SetVendorStatus(vendorStatus, status);
        if (status == hixl::SUCCESS) {
            context->initialized = true;
        }
        return ToDsResult(status);
    });
}

DsHixlResult ConnectEngine(DsHixlEngineHandle engine, DsHixlStringView remoteEndpoint, int32_t timeoutMs,
                           uint32_t *vendorStatus) noexcept
{
    return InvokeNoexcept([=]() {
        auto *context = GetContext(engine);
        SetVendorStatus(vendorStatus, hixl::SUCCESS);
        if (context == nullptr || context->engine == nullptr || !context->initialized
            || !IsValidString(remoteEndpoint, false) || timeoutMs <= 0) {
            return DS_HIXL_INVALID_ARGUMENT;
        }
        hixl::Status status = context->engine->Connect(ToAscendString(remoteEndpoint), timeoutMs);
        SetVendorStatus(vendorStatus, status);
        return status == hixl::ALREADY_CONNECTED ? DS_HIXL_OK : ToDsResult(status);
    });
}

DsHixlResult DisconnectEngine(DsHixlEngineHandle engine, DsHixlStringView remoteEndpoint, int32_t timeoutMs,
                              uint32_t *vendorStatus) noexcept
{
    return InvokeNoexcept([=]() {
        auto *context = GetContext(engine);
        SetVendorStatus(vendorStatus, hixl::SUCCESS);
        if (context == nullptr || context->engine == nullptr || !context->initialized
            || !IsValidString(remoteEndpoint, false) || timeoutMs <= 0) {
            return DS_HIXL_INVALID_ARGUMENT;
        }
        hixl::Status status = context->engine->Disconnect(ToAscendString(remoteEndpoint), timeoutMs);
        SetVendorStatus(vendorStatus, status);
        return ToDsResult(status);
    });
}

DsHixlResult RegisterMemory(DsHixlEngineHandle engine, const DsHixlRegisterMemoryRequest *request,
                            DsHixlMemHandle *memoryHandle, uint32_t *vendorStatus) noexcept
{
    return InvokeNoexcept([=]() {
        auto *context = GetContext(engine);
        SetVendorStatus(vendorStatus, hixl::SUCCESS);
        if (memoryHandle == nullptr) {
            return DS_HIXL_INVALID_ARGUMENT;
        }
        *memoryHandle = nullptr;
        if (context == nullptr || context->engine == nullptr || !context->initialized || request == nullptr
            || request->address == 0 || request->length == 0 || !FitsSizeT(request->length)
            || (request->memoryType != DS_HIXL_MEMORY_DEVICE && request->memoryType != DS_HIXL_MEMORY_HOST)) {
            return DS_HIXL_INVALID_ARGUMENT;
        }
        hixl::MemDesc descriptor{ request->address, static_cast<size_t>(request->length) };
        hixl::MemHandle handle = nullptr;
        hixl::MemType type = request->memoryType == DS_HIXL_MEMORY_DEVICE ? hixl::MEM_DEVICE : hixl::MEM_HOST;
        hixl::Status status = context->engine->RegisterMem(descriptor, type, handle);
        SetVendorStatus(vendorStatus, status);
        if (status == hixl::SUCCESS) {
            if (handle == nullptr) {
                return DS_HIXL_RUNTIME_ERROR;
            }
            *memoryHandle = reinterpret_cast<DsHixlMemHandle>(handle);
        }
        return ToDsResult(status);
    });
}

DsHixlResult DeregisterMemory(DsHixlEngineHandle engine, DsHixlMemHandle memoryHandle,
                              uint32_t *vendorStatus) noexcept
{
    return InvokeNoexcept([=]() {
        auto *context = GetContext(engine);
        SetVendorStatus(vendorStatus, hixl::SUCCESS);
        if (context == nullptr || context->engine == nullptr || !context->initialized || memoryHandle == nullptr) {
            return DS_HIXL_INVALID_ARGUMENT;
        }
        hixl::Status status = context->engine->DeregisterMem(reinterpret_cast<hixl::MemHandle>(memoryHandle));
        SetVendorStatus(vendorStatus, status);
        return ToDsResult(status);
    });
}

DsHixlResult TransferSync(DsHixlEngineHandle engine, const DsHixlTransferRequest *request,
                          uint32_t *vendorStatus) noexcept
{
    return InvokeNoexcept([=]() {
        auto *context = GetContext(engine);
        SetVendorStatus(vendorStatus, hixl::SUCCESS);
        if (request != nullptr && request->descriptorCount == 0) {
            return DS_HIXL_OK;
        }
        if (context == nullptr || context->engine == nullptr || !context->initialized
            || request == nullptr || !IsValidString(request->remoteEndpoint, false) || request->descriptors == nullptr
            || request->timeoutMs <= 0
            || (request->operation != DS_HIXL_TRANSFER_READ && request->operation != DS_HIXL_TRANSFER_WRITE)) {
            return DS_HIXL_INVALID_ARGUMENT;
        }

        context->transferDescriptors.clear();
        context->transferDescriptors.reserve(request->descriptorCount);
        for (uint32_t i = 0; i < request->descriptorCount; ++i) {
            const auto &descriptor = request->descriptors[i];
            if (descriptor.localAddr == 0 || descriptor.remoteAddr == 0 || descriptor.length == 0
                || !FitsSizeT(descriptor.length)) {
                context->transferDescriptors.clear();
                return DS_HIXL_INVALID_ARGUMENT;
            }
            context->transferDescriptors.push_back(hixl::TransferOpDesc{ descriptor.localAddr, descriptor.remoteAddr,
                                                                         static_cast<size_t>(descriptor.length) });
        }

        hixl::TransferOp transferOperation =
            request->operation == DS_HIXL_TRANSFER_READ ? hixl::READ : hixl::WRITE;
        hixl::Status status = context->engine->TransferSync(ToAscendString(request->remoteEndpoint), transferOperation,
                                                            context->transferDescriptors, request->timeoutMs);
        context->transferDescriptors.clear();
        SetVendorStatus(vendorStatus, status);
        return ToDsResult(status);
    });
}

const DsHixlApi HIXL_API_V1 = {
    DS_HIXL_ABI_VERSION_1,
    sizeof(DsHixlApi),
    &CreateEngine,
    &FinalizeEngine,
    &DestroyEngine,
    &InitializeEngine,
    &ConnectEngine,
    &DisconnectEngine,
    &RegisterMemory,
    &DeregisterMemory,
    &TransferSync,
};

}  // namespace

extern "C" DS_HIXL_EXPORT DsHixlResult DsHixlGetApi(uint32_t requestedAbiVersion, const DsHixlApi **api)
{
    if (api == nullptr) {
        return DS_HIXL_INVALID_ARGUMENT;
    }
    *api = nullptr;
    if (requestedAbiVersion != DS_HIXL_ABI_VERSION_1) {
        return DS_HIXL_NOT_SUPPORTED;
    }
    *api = &HIXL_API_V1;
    return DS_HIXL_OK;
}
