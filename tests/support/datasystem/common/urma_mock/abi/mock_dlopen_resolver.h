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

#ifndef DATASYSTEM_COMMON_URMA_MOCK_ABI_MOCK_DLOPEN_RESOLVER_H
#define DATASYSTEM_COMMON_URMA_MOCK_ABI_MOCK_DLOPEN_RESOLVER_H

#include <string>

namespace datasystem {
namespace urma_mock {
/**
 * @brief Return the sentinel handle used by the dlopen shim in URMA mock mode.
 * @return Opaque mock dlopen handle.
 */
void *MockUrmaDlopenHandle();

/**
 * @brief Check whether a dlopen handle is the mock sentinel.
 * @param[in] handle Handle to test.
 * @return true if the handle belongs to the mock resolver.
 */
bool IsMockUrmaDlopenHandle(void *handle);

/**
 * @brief Initialize the mock backend behind the dlopen shim.
 * @return true if the backend is ready.
 */
bool InitMockUrmaDlopen();

/**
 * @brief Clean up the mock backend behind the dlopen shim.
 */
void CleanupMockUrmaDlopen();

/**
 * @brief Resolve a URMA SDK symbol name to its mock ABI entry point.
 * @param[in] name URMA SDK symbol name.
 * @return Function pointer for the mock symbol, or nullptr if not supported.
 */
void *LoadMockUrmaSymbol(const std::string &name);

}  // namespace urma_mock
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_URMA_MOCK_ABI_MOCK_DLOPEN_RESOLVER_H
