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

#include <cstddef>
#include <string>

namespace datasystem {

void *GetCudaRuntimeSymbol(const std::string &name);

/**
 * @brief Register host memory when CUDA headers and runtime are available.
 * @param[in] pointer Host memory address.
 * @param[in] size Host memory size.
 */
void RegisterCudaHostMemory(void *pointer, size_t size);

void UnregisterCudaHostMemory(void *pointer);

}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_DEVICE_NVIDIA_CUDA_HOST_MEMORY_H
