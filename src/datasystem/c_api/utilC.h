/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: some common CPP functions for the wrapper
 */
#ifndef DATASYSTEM_UTIL_C_CLIENT_C_WRAPPER_H
#define DATASYSTEM_UTIL_C_CLIENT_C_WRAPPER_H

#include <stddef.h>

#include "datasystem/c_api/status_definition.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Creates a C style array of strings (also allocates memory)
 * @param[in] input arrlen - array length
 * @return Pointer to array of strings(char*).
 */
char **MakeCharsArray(int arrLen);

/**
 * @brief Creates a C style array of size_t (also allocates memory)
 * @param[in] input arrlen - array length
 * @return Pointer to array of size_t.
 */
size_t *MakeNumArray(int arrLen);

/**
 * @brief Gets a string(char*) from array of strings at index idx
 * @param[in] input arr - array of strings
 * @param[in] input idx - index in the string
 * @return Pointer to C string
 */
char *GetCharsAtIdx(char **arr, int idx);

/**
 * @brief Gets a uint32_t from array of size_t at index idx
 * @param[in] input arr - array of size_t
 * @param[in] input idx - index in the size_t
 * @return number
 */
size_t GetNumAtIdx(size_t *arr, int idx);

/**
 * @brief Sets a string(char*) in array of strings at index idx
 * @param[in] input arr - array of strings
 * @param[in] input str - C string
 * @param[in] input idx - index in the string
 * @return
 */
void SetCharsAtIdx(char **arr, char *str, int idx);

/**
 * @brief Deallocate memory from the array of strings
 * @param[in] input arr - array of strings
 * @param[in] input arrlen - number of strings
 * @return
 */
void FreeCharsArray(char **arr, int arrLen);

/**
 * @brief Deallocate memory from the array of size_t
 * @param[in] arr Array of size_t
 * @return
 */
void FreeNumArray(size_t *arr);

/**
 * @brief Set trace id for all API calls of the current thread.
 * @param [in] cTraceId The trace id.
 * @param [in] traceIdLen The length of trace id.
 * @return status of the call
 */
struct StatusC ContextSetTraceId(const char *cTraceId, size_t traceIdLen);

/**
 * @brief Set tenantId for all API calls of the current thread.
 * @param [in] cTenantId The tenant id.
 * @param [in] tenantIdLen The length of tenant id.
 */
void ContextSetTenantId(const char *cTenantId, size_t tenantIdLen);

#ifdef __cplusplus
};
#endif
#endif