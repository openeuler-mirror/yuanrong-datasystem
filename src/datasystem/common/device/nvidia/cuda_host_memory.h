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
#ifndef DATASYSTEM_COMMON_DEVICE_NVIDIA_CUDA_HOST_MEMORY_H
#define DATASYSTEM_COMMON_DEVICE_NVIDIA_CUDA_HOST_MEMORY_H

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>

#include "datasystem/utils/cuda_funcs.h"
#include "datasystem/utils/status.h"

namespace datasystem {

constexpr int64_t CUDA_SLOW_OPERATION_THRESHOLD_US = 100000;
constexpr int64_t CUDA_SLOW_LOG_INTERVAL_US = 10000000;

struct CudaSlowLogState {
    std::atomic<int64_t> nextAllowedUs{ std::numeric_limits<int64_t>::min() };
    std::atomic<uint64_t> suppressedCount{ 0 };
    std::atomic<int64_t> suppressedMaxUs{ 0 };
};

bool TryAcquireCudaSlowLog(CudaSlowLogState &state, int64_t elapsedUs, uint64_t &suppressedCount,
                           int64_t &suppressedMaxUs,
                           std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now());

void *GetCudaRuntimeSymbol(const std::string &name);

void RegisterCudaFuncs(const CudaFuncs &funcs);

bool IsCudaHostMemoryRegistrationEnabled();

/**
 * @brief Register host memory through the application-provided callback when available.
 * @param[in] pointer Host memory address.
 * @param[in] size Host memory size.
 * @return True if the callback succeeds.
 */
bool RegisterCudaHostMemory(void *pointer, size_t size);

/**
 * @brief Unregister host memory through the application-provided callback when available.
 * @param[in] pointer Host memory address.
 * @return True if the callback succeeds.
 */
bool UnregisterCudaHostMemory(void *pointer);

Status DsCudaMemcpyAsync(void *dst, const void *src, size_t size, DsCudaMemcpyKind kind, void *stream);

}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_DEVICE_NVIDIA_CUDA_HOST_MEMORY_H
