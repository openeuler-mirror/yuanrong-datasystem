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
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <mutex>
#include <string>
#include <unordered_map>

#include "datasystem/common/log/log.h"

namespace datasystem {

bool TryAcquireCudaSlowLog(CudaSlowLogState &state, int64_t elapsedUs, uint64_t &suppressedCount,
                           int64_t &suppressedMaxUs,
                           std::chrono::steady_clock::time_point now)
{
    const auto nowUs = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    auto nextAllowedUs = state.nextAllowedUs.load(std::memory_order_relaxed);
    if (nowUs < nextAllowedUs
        || !state.nextAllowedUs.compare_exchange_strong(nextAllowedUs, nowUs + CUDA_SLOW_LOG_INTERVAL_US,
                                                       std::memory_order_relaxed)) {
        state.suppressedCount.fetch_add(1, std::memory_order_relaxed);
        auto maxUs = state.suppressedMaxUs.load(std::memory_order_relaxed);
        if (elapsedUs > maxUs) {
            // Best effort: do not retry on contention for diagnostic-only statistics.
            (void)state.suppressedMaxUs.compare_exchange_strong(maxUs, elapsedUs, std::memory_order_relaxed);
        }
        return false;
    }
    // Concurrent counts and maxima may be attributed to adjacent emissions independently.
    suppressedCount = state.suppressedCount.exchange(0, std::memory_order_relaxed);
    suppressedMaxUs = state.suppressedMaxUs.exchange(0, std::memory_order_relaxed);
    return true;
}

namespace {
void LogSlowHostMemoryCallback(bool registering, void *pointer, size_t size, int64_t elapsedUs, int rc,
                               bool callbackException = false)
{
    if (elapsedUs <= CUDA_SLOW_OPERATION_THRESHOLD_US) {
        return;
    }
    static CudaSlowLogState registerLimiter;
    static CudaSlowLogState unregisterLimiter;
    auto &limiter = registering ? registerLimiter : unregisterLimiter;
    uint64_t suppressedCount = 0;
    int64_t suppressedMaxUs = 0;
    if (!TryAcquireCudaSlowLog(limiter, elapsedUs, suppressedCount, suppressedMaxUs)) {
        return;
    }
    const auto endTimestampUs = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    LOG(INFO) << "[CUDA_HOST_SLOW] operation=" << (registering ? "register" : "unregister")
              << " pointer=" << pointer << " size=" << size
              << " callback_us=" << elapsedUs << " end_timestamp_us=" << endTimestampUs
              << " rc=" << rc << " callback_exception=" << callbackException
              << " suppressed_count=" << suppressedCount << " suppressed_max_us=" << suppressedMaxUs;
}

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

    void RegisterCudaFuncs(const CudaFuncs &funcs)
    {
        if (funcs.hostRegister == nullptr || funcs.hostUnregister == nullptr || funcs.getErrorString == nullptr
            || funcs.memcpyAsync == nullptr) {
            LOG(WARNING) << "[CudaHostMemory] Ignore invalid CUDA callback registration because hostRegister, "
                            "hostUnregister, getErrorString and memcpyAsync must all be non-null";
            return;
        }
        bool alreadyRegistered = false;
        {
            std::lock_guard<std::mutex> lock(funcsMutex_);
            if (funcsRegistered_.load(std::memory_order_relaxed)) {
                alreadyRegistered = true;
            } else {
                funcs_ = funcs;
                funcsRegistered_.store(true, std::memory_order_release);
            }
        }
        if (alreadyRegistered) {
            LOG(WARNING) << "[CudaHostMemory] Ignore repeated CUDA callback registration because the process-wide "
                            "callbacks are already frozen";
        }
    }

    bool IsHostMemoryRegistrationEnabled() const
    {
        const auto funcs = GetCudaFuncsSnapshot();
        return funcs.hostRegister != nullptr && funcs.hostUnregister != nullptr;
    }

    bool Register(void *pointer, size_t size)
    {
        const auto funcs = GetCudaFuncsSnapshot();
        if (funcs.hostRegister == nullptr) {
            WarnNotRegistered();
            return false;
        }
        if (pointer == nullptr || size == 0) {
            LOG(ERROR) << "[CudaHostMemory] Invalid CUDA host memory range, pointer: " << pointer << ", size: " << size;
            return false;
        }
        VLOG(1) << "[CudaHostMemory] cudaHostRegister started, pointer: " << pointer << ", size: " << size;
        int rc = kCudaSuccess;
        const auto begin = std::chrono::steady_clock::now();
        try {
            rc = funcs.hostRegister(pointer, size, kCudaHostRegisterPortable);
        } catch (const std::exception &e) {
            const auto elapsedUs =
                std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - begin);
            LogSlowHostMemoryCallback(true, pointer, size, elapsedUs.count(), rc, true);
            VLOG(1) << "[CudaHostMemory] cudaHostRegister finished, pointer: " << pointer << ", size: " << size
                    << ", elapsedUs: " << elapsedUs.count() << ", callbackException: true";
            LOG(ERROR) << "[CudaHostMemory] cudaHostRegister callback threw an exception, pointer: " << pointer
                       << ", size: " << size << ", error: " << e.what();
            return false;
        } catch (...) {
            const auto elapsedUs =
                std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - begin);
            LogSlowHostMemoryCallback(true, pointer, size, elapsedUs.count(), rc, true);
            VLOG(1) << "[CudaHostMemory] cudaHostRegister finished, pointer: " << pointer << ", size: " << size
                    << ", elapsedUs: " << elapsedUs.count() << ", callbackException: true";
            LOG(ERROR) << "[CudaHostMemory] cudaHostRegister callback threw an unknown exception, pointer: " << pointer
                       << ", size: " << size;
            return false;
        }
        auto elapsedUs =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - begin);
        LogSlowHostMemoryCallback(true, pointer, size, elapsedUs.count(), rc);
        VLOG(1) << "[CudaHostMemory] cudaHostRegister finished, pointer: " << pointer << ", size: " << size
                << ", elapsedUs: " << elapsedUs.count() << ", return: " << rc;
        if (rc != kCudaSuccess && rc != kCudaErrorHostMemoryAlreadyRegistered) {
            LOG(ERROR) << "[CudaHostMemory] cudaHostRegister failed, pointer: " << pointer << ", size: " << size
                       << ", return: " << rc
                       << ", error: " << GetErrorString(funcs, rc);
            return false;
        }
        return true;
    }

    bool Unregister(void *pointer)
    {
        const auto funcs = GetCudaFuncsSnapshot();
        if (funcs.hostUnregister == nullptr) {
            WarnNotRegistered();
            return false;
        }
        if (pointer == nullptr) {
            LOG(ERROR) << "[CudaHostMemory] Invalid CUDA host memory unregister pointer: " << pointer;
            return false;
        }
        VLOG(1) << "[CudaHostMemory] cudaHostUnregister started, pointer: " << pointer;
        int rc = kCudaSuccess;
        const auto begin = std::chrono::steady_clock::now();
        try {
            rc = funcs.hostUnregister(pointer);
        } catch (const std::exception &e) {
            const auto elapsedUs =
                std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - begin);
            LogSlowHostMemoryCallback(false, pointer, 0, elapsedUs.count(), rc, true);
            VLOG(1) << "[CudaHostMemory] cudaHostUnregister finished, pointer: " << pointer
                    << ", elapsedUs: " << elapsedUs.count() << ", callbackException: true";
            LOG(ERROR) << "[CudaHostMemory] cudaHostUnregister callback threw an exception, pointer: " << pointer
                       << ", error: " << e.what();
            return false;
        } catch (...) {
            const auto elapsedUs =
                std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - begin);
            LogSlowHostMemoryCallback(false, pointer, 0, elapsedUs.count(), rc, true);
            VLOG(1) << "[CudaHostMemory] cudaHostUnregister finished, pointer: " << pointer
                    << ", elapsedUs: " << elapsedUs.count() << ", callbackException: true";
            LOG(ERROR) << "[CudaHostMemory] cudaHostUnregister callback threw an unknown exception, pointer: "
                       << pointer;
            return false;
        }
        auto elapsedUs =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - begin);
        LogSlowHostMemoryCallback(false, pointer, 0, elapsedUs.count(), rc);
        VLOG(1) << "[CudaHostMemory] cudaHostUnregister finished, pointer: " << pointer
                << ", elapsedUs: " << elapsedUs.count() << ", return: " << rc;
        if (rc != kCudaSuccess) {
            LOG(ERROR) << "[CudaHostMemory] cudaHostUnregister failed, pointer: " << pointer << ", return: " << rc
                       << ", error: " << GetErrorString(funcs, rc);
            return false;
        }
        return true;
    }

    Status MemcpyAsync(void *dst, const void *src, size_t size, DsCudaMemcpyKind kind, void *stream)
    {
        const auto funcs = GetCudaFuncsSnapshot();
        if (funcs.memcpyAsync == nullptr) {
            return Status(K_NOT_SUPPORTED, "CUDA memcpyAsync callback is not registered");
        }
        int rc = kCudaSuccess;
        const auto begin = std::chrono::steady_clock::now();
        const auto logSlow = [begin, kind, dst, src, size, stream, &rc](bool callbackException = false) {
            const auto elapsedUs = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - begin).count();
            if (elapsedUs > CUDA_SLOW_OPERATION_THRESHOLD_US) {
                static CudaSlowLogState h2dLimiter;
                static CudaSlowLogState d2hLimiter;
                auto &limiter = kind == DsCudaMemcpyKind::HOST_TO_DEVICE ? h2dLimiter : d2hLimiter;
                uint64_t suppressedCount = 0;
                int64_t suppressedMaxUs = 0;
                if (!TryAcquireCudaSlowLog(limiter, elapsedUs, suppressedCount, suppressedMaxUs)) {
                    return;
                }
                const auto endTimestampUs = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                LOG(INFO) << "[CUDA_MEMCPY_SLOW] direction="
                          << (kind == DsCudaMemcpyKind::HOST_TO_DEVICE ? "H2D" : "D2H")
                          << " dst=" << dst << " src=" << src << " size=" << size << " stream=" << stream
                          << " callback_us=" << elapsedUs << " end_timestamp_us=" << endTimestampUs
                          << " rc=" << rc << " callback_exception=" << callbackException
                          << " suppressed_count=" << suppressedCount << " suppressed_max_us=" << suppressedMaxUs;
            }
        };
        try {
            rc = funcs.memcpyAsync(dst, src, size, kind, stream);
        } catch (const std::exception &e) {
            logSlow(true);
            return Status(K_RUNTIME_ERROR, std::string("CUDA memcpyAsync callback threw an exception: ") + e.what());
        } catch (...) {
            logSlow(true);
            return Status(K_RUNTIME_ERROR, "CUDA memcpyAsync callback threw an unknown exception");
        }
        logSlow();
        if (rc != kCudaSuccess) {
            return Status(K_RUNTIME_ERROR,
                          "CUDA memcpyAsync failed, return: " + std::to_string(rc) +
                              ", error: " + GetErrorString(funcs, rc));
        }
        return Status::OK();
    }

private:
    CudaFuncs GetCudaFuncsSnapshot() const
    {
        if (!funcsRegistered_.load(std::memory_order_acquire)) {
            return {};
        }
        return funcs_;
    }

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
        });
    }

    void WarnNotRegistered()
    {
        std::call_once(warnOnce_, []() {
            LOG(WARNING) << "[CudaHostMemory] CUDA host memory functions not registered, call "
                            "KVClient::RegisterCudaFuncs() to enable host memory registration";
        });
    }

    std::string GetErrorString(const CudaFuncs &funcs, int rc) const
    {
        if (funcs.getErrorString == nullptr) {
            return std::to_string(rc);
        }
        try {
            const char *message = funcs.getErrorString(rc);
            return message == nullptr ? std::to_string(rc) : std::string(message);
        } catch (const std::exception &e) {
            LOG(WARNING) << "[CudaHostMemory] CUDA getErrorString callback threw an exception, return: " << rc
                         << ", error: " << e.what();
        } catch (...) {
            LOG(WARNING) << "[CudaHostMemory] CUDA getErrorString callback threw an unknown exception, return: " << rc;
        }
        return std::to_string(rc);
    }

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
    std::once_flag warnOnce_;
    void *handle_{ nullptr };
    std::mutex symbolMutex_;
    std::unordered_map<std::string, void *> symbols_;
    std::mutex funcsMutex_;
    CudaFuncs funcs_{};
    std::atomic<bool> funcsRegistered_{ false };
};
}  // namespace

void *GetCudaRuntimeSymbol(const std::string &name)
{
    return CudaRuntimeApi::Instance().GetSymbol(name);
}

void RegisterCudaFuncs(const CudaFuncs &funcs)
{
    CudaRuntimeApi::Instance().RegisterCudaFuncs(funcs);
}

bool IsCudaHostMemoryRegistrationEnabled()
{
    return CudaRuntimeApi::Instance().IsHostMemoryRegistrationEnabled();
}

bool RegisterCudaHostMemory(void *pointer, size_t size)
{
    return CudaRuntimeApi::Instance().Register(pointer, size);
}

bool UnregisterCudaHostMemory(void *pointer)
{
    return CudaRuntimeApi::Instance().Unregister(pointer);
}

Status DsCudaMemcpyAsync(void *dst, const void *src, size_t size, DsCudaMemcpyKind kind, void *stream)
{
    return CudaRuntimeApi::Instance().MemcpyAsync(dst, src, size, kind, stream);
}

}  // namespace datasystem
