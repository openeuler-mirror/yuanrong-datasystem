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

/**
 * Description: CUDA host memory registration independent of RH2D.
 */
#include "datasystem/common/device/nvidia/cuda_host_memory.h"

#include <dlfcn.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

#include "datasystem/common/log/log.h"

#if __has_include(<cuda_runtime_api.h>)
#include <cuda_runtime_api.h>
#define DATASYSTEM_HAS_CUDA_RUNTIME_API
#endif

namespace datasystem {
namespace {
#ifdef DATASYSTEM_HAS_CUDA_RUNTIME_API
using HostRegisterFunc = decltype(&cudaHostRegister);
using HostUnregisterFunc = decltype(&cudaHostUnregister);
using GetErrorStringFunc = decltype(&cudaGetErrorString);
#endif

class CudaRuntimeApi {
public:
    static CudaRuntimeApi &Instance()
    {
        static auto *instance = new CudaRuntimeApi();
        return *instance;
    }

    void *GetSymbol(const std::string &name)
    {
        Load();
        return name.empty() || handle_ == nullptr ? nullptr : LoadSymbol<void *>(name);
    }

    void Register(void *pointer, size_t size)
    {
#ifndef DATASYSTEM_HAS_CUDA_RUNTIME_API
        (void)pointer;
        (void)size;
#else
        Load();
        if (hostRegister_ == nullptr) {
            return;
        }
        if (pointer == nullptr || size == 0) {
            LOG(ERROR) << "[CudaHostMemory] Invalid CUDA host memory range, pointer: " << pointer << ", size: " << size;
            return;
        }
        auto begin = std::chrono::steady_clock::now();
        cudaError_t rc = hostRegister_(pointer, size, cudaHostRegisterPortable);
        auto elapsedUs =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - begin);
        LOG(INFO) << "[CudaHostMemory] cudaHostRegister finished, pointer: " << pointer << ", size: " << size
                  << ", elapsedUs: " << elapsedUs.count() << ", return: " << static_cast<int>(rc);
        if (rc != cudaSuccess && rc != cudaErrorHostMemoryAlreadyRegistered) {
            LOG(ERROR) << "[CudaHostMemory] cudaHostRegister failed, return: " << static_cast<int>(rc)
                       << ", error: " << GetErrorString(rc);
        }
#endif
    }

    void Unregister(void *pointer)
    {
#ifdef DATASYSTEM_HAS_CUDA_RUNTIME_API
        Load();
        if (pointer != nullptr && hostUnregister_ != nullptr) {
            auto begin = std::chrono::steady_clock::now();
            cudaError_t rc = hostUnregister_(pointer);
            auto elapsedUs =
                std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - begin);
            LOG(INFO) << "[CudaHostMemory] cudaHostUnregister finished, pointer: " << pointer
                      << ", elapsedUs: " << elapsedUs.count() << ", return: " << static_cast<int>(rc);
            if (rc != cudaSuccess) {
                LOG(ERROR) << "[CudaHostMemory] cudaHostUnregister failed, return: " << static_cast<int>(rc)
                           << ", error: " << GetErrorString(rc);
            }
        }
#else
        (void)pointer;
#endif
    }

private:
    void Load()
    {
        std::call_once(loadOnce_, [this]() {
            const std::array<std::string, 4> names = {
                "libcudart.so", "libcudart.so.13", "libcudart.so.12", "libcudart.so.11.0"
            };
            for (const auto &name : names) {
                handle_ = dlopen(name.c_str(), RTLD_LAZY | RTLD_LOCAL);
                if (handle_ != nullptr) {
                    break;
                }
            }
#ifdef DATASYSTEM_HAS_CUDA_RUNTIME_API
            LoadHostMemorySymbols();
#endif
        });
    }

#ifdef DATASYSTEM_HAS_CUDA_RUNTIME_API
    void LoadHostMemorySymbols()
    {
        if (handle_ == nullptr) {
            return;
        }
        auto hostRegister = LoadSymbol<HostRegisterFunc>("cudaHostRegister");
        auto hostUnregister = LoadSymbol<HostUnregisterFunc>("cudaHostUnregister");
        if (hostRegister != nullptr && hostUnregister != nullptr) {
            hostRegister_ = hostRegister;
            hostUnregister_ = hostUnregister;
        }
        getErrorString_ = LoadSymbol<GetErrorStringFunc>("cudaGetErrorString");
    }

    std::string GetErrorString(cudaError_t rc) const
    {
        if (getErrorString_ == nullptr) {
            return std::to_string(static_cast<int>(rc));
        }
        auto message = getErrorString_(rc);
        return message == nullptr ? std::to_string(static_cast<int>(rc)) : std::string(message);
    }
#endif

    template <typename T>
    T LoadSymbol(const std::string &name)
    {
        std::lock_guard<std::mutex> lock(symbolMutex_);
        auto iter = symbols_.find(name);
        if (iter == symbols_.end()) {
            void *symbol = dlsym(handle_, name.c_str());
            iter = symbols_.emplace(name, symbol).first;
        }
        return reinterpret_cast<T>(reinterpret_cast<intptr_t>(iter->second));
    }

    std::once_flag loadOnce_;
    void *handle_{ nullptr };
    std::mutex symbolMutex_;
    std::unordered_map<std::string, void *> symbols_;
#ifdef DATASYSTEM_HAS_CUDA_RUNTIME_API
    HostRegisterFunc hostRegister_{ nullptr };
    HostUnregisterFunc hostUnregister_{ nullptr };
    GetErrorStringFunc getErrorString_{ nullptr };
#endif
};
}  // namespace

void *GetCudaRuntimeSymbol(const std::string &name)
{
    return CudaRuntimeApi::Instance().GetSymbol(name);
}

void RegisterCudaHostMemory(void *pointer, size_t size)
{
    CudaRuntimeApi::Instance().Register(pointer, size);
}

void UnregisterCudaHostMemory(void *pointer)
{
    CudaRuntimeApi::Instance().Unregister(pointer);
}

}  // namespace datasystem

#undef DATASYSTEM_HAS_CUDA_RUNTIME_API
