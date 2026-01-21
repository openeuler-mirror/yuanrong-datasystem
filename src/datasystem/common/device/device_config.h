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
 * Description: Device backend configuration macros.
 * This file defines compile-time macros for selecting the device backend.
 * Similar to flags.h, these are currently hardcoded but should be set by CMake
 * in production builds.
 * Configuration Options:
 *   - USE_NPU: Enable NPU (Ascend/ACL) backend
 *   - USE_GPU: Enable GPU (NVIDIA/CUDA) backend
 * Note: Only one backend can be enabled at a time.
 */
#ifndef DATASYSTEM_COMMON_DEVICE_DEVICE_CONFIG_H
#define DATASYSTEM_COMMON_DEVICE_DEVICE_CONFIG_H

#define USE_NPU true

#define USE_GPU false

#endif  // DATASYSTEM_COMMON_DEVICE_DEVICE_CONFIG_H
