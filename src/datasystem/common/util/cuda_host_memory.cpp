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
#include "datasystem/common/util/cuda_host_memory.h"

#include <dlfcn.h>

#include <atomic>
#include <array>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

namespace datasystem {
namespace {
constexpr int CUDA_SUCCESS = 0;
constexpr int CUDA_ERROR_INITIALIZATION = 3;
constexpr int CUDA_ERROR_STUB_LIBRARY = 34;
constexpr int CUDA_ERROR_INSUFFICIENT_DRIVER = 35;
constexpr int CUDA_ERROR_NO_DEVICE = 100;
constexpr int CUDA_ERROR_STARTUP_FAILURE = 127;
constexpr int CUDA_ERROR_HOST_MEMORY_ALREADY_REGISTERED = 712;
constexpr int CUDA_ERROR_SYSTEM_NOT_READY = 802;
constexpr int CUDA_ERROR_SYSTEM_DRIVER_MISMATCH = 803;
constexpr unsigned int CUDA_HOST_REGISTER_PORTABLE = 0x01;

bool IsCudaUnavailable(int rc)
{
    return rc == CUDA_ERROR_INITIALIZATION || rc == CUDA_ERROR_STUB_LIBRARY || rc == CUDA_ERROR_INSUFFICIENT_DRIVER ||
           rc == CUDA_ERROR_NO_DEVICE || rc == CUDA_ERROR_STARTUP_FAILURE || rc == CUDA_ERROR_SYSTEM_NOT_READY ||
           rc == CUDA_ERROR_SYSTEM_DRIVER_MISMATCH;
}

using HostRegisterFunc = int (*)(void *, size_t, unsigned int);
using HostUnregisterFunc = int (*)(void *);
using GetErrorStringFunc = const char *(*)(int);

class CudaRuntimeApi {
public:
    ~CudaRuntimeApi()
    {
        if (handle_ != nullptr) {
            dlclose(handle_);
        }
    }

    static CudaRuntimeApi &Instance()
    {
        static auto *instance = new CudaRuntimeApi();
        return *instance;
    }

    void *GetSymbol(const char *name)
    {
        Load();
        return name == nullptr || handle_ == nullptr ? nullptr : LoadSymbol<void *>(name);
    }

    Status Register(void *pointer, size_t size)
    {
        Load();
        if (hostRegister_ == nullptr || unavailable_.load(std::memory_order_acquire)) {
            return Status::OK();
        }
        if (pointer == nullptr || size == 0) {
            return Status(K_INVALID, "Invalid CUDA host memory range");
        }
        int rc = hostRegister_(pointer, size, CUDA_HOST_REGISTER_PORTABLE);
        if (rc == CUDA_SUCCESS || rc == CUDA_ERROR_HOST_MEMORY_ALREADY_REGISTERED) {
            return Status::OK();
        }
        if (IsCudaUnavailable(rc)) {
            unavailable_.store(true, std::memory_order_release);
            return Status::OK();
        }
        return Status(K_RUNTIME_ERROR, "cudaHostRegister failed: " + GetErrorString(rc));
    }

    void Unregister(void *pointer)
    {
        Load();
        if (pointer != nullptr && hostUnregister_ != nullptr && !unavailable_.load(std::memory_order_acquire)) {
            (void)hostUnregister_(pointer);
        }
    }

private:
    void Load()
    {
        std::call_once(loadOnce_, [this]() {
            constexpr std::array<const char *, 4> names = {
                "libcudart.so", "libcudart.so.13", "libcudart.so.12", "libcudart.so.11.0"
            };
            for (const auto *name : names) {
                handle_ = dlopen(name, RTLD_LAZY | RTLD_LOCAL);
                if (handle_ != nullptr) {
                    break;
                }
            }
            if (handle_ != nullptr) {
                auto hostRegister = LoadSymbol<HostRegisterFunc>("cudaHostRegister");
                auto hostUnregister = LoadSymbol<HostUnregisterFunc>("cudaHostUnregister");
                if (hostRegister != nullptr && hostUnregister != nullptr) {
                    hostRegister_ = hostRegister;
                    hostUnregister_ = hostUnregister;
                }
                getErrorString_ = LoadSymbol<GetErrorStringFunc>("cudaGetErrorString");
            }
        });
    }

    template <typename T>
    T LoadSymbol(const char *name)
    {
        std::lock_guard<std::mutex> lock(symbolMutex_);
        auto iter = symbols_.find(name);
        if (iter == symbols_.end()) {
            void *symbol = dlsym(handle_, name);
            iter = symbols_.emplace(name, symbol).first;
        }
        return reinterpret_cast<T>(reinterpret_cast<intptr_t>(iter->second));
    }

    std::string GetErrorString(int rc) const
    {
        const char *message = getErrorString_ == nullptr ? nullptr : getErrorString_(rc);
        return message == nullptr ? std::to_string(rc) : message;
    }

    std::once_flag loadOnce_;
    void *handle_{ nullptr };
    std::mutex symbolMutex_;
    std::unordered_map<std::string, void *> symbols_;
    HostRegisterFunc hostRegister_{ nullptr };
    HostUnregisterFunc hostUnregister_{ nullptr };
    GetErrorStringFunc getErrorString_{ nullptr };
    std::atomic<bool> unavailable_{ false };
};
}  // namespace

void *GetCudaRuntimeSymbol(const char *name)
{
    return CudaRuntimeApi::Instance().GetSymbol(name);
}

Status RegisterCudaHostMemory(void *pointer, size_t size)
{
    return CudaRuntimeApi::Instance().Register(pointer, size);
}

void UnregisterCudaHostMemory(void *pointer)
{
    CudaRuntimeApi::Instance().Unregister(pointer);
}

}  // namespace datasystem
