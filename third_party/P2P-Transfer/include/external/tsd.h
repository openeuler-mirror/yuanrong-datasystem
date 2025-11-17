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
#ifndef TSD_H
#define TSD_H

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Open Tsd interface
 * @param [in] logicDeviceId physical device ID
 * @param [in] rankSize: Training rank size (default 1). If greater than 1, HCCP
 * is started to for communication related operations
 * @return TSD_OK : success
 */
uint32_t TsdOpen(const uint32_t logicDeviceId, const uint32_t rankSize);

/**
* @brief Instructs the TsdClient to close resources.
* @param [in] logicDeviceId physical device ID
* @return TSD_OK : success
*/
uint32_t TsdClose(const uint32_t logicDeviceId);

#ifdef __cplusplus
}
#endif
#endif // TSD_H