/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Generator for uuid.
 */

#ifndef DATASYSTEM_COMMON_UTIL_UUID_GENERATOR_H
#define DATASYSTEM_COMMON_UTIL_UUID_GENERATOR_H

#include <string>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
static constexpr size_t UUID_SIZE = 16;
static constexpr size_t UUID_VERSION_BYTEINDEX = 6;
static constexpr size_t UUID_VARIANT_BYTEINDEX = 8;

/**
 * @brief Convert a 16-byte uuid to a 36-byte printable uuid
 * @param[in] bytesUuid A 16-byte uuid.
 * @return Return 36-byte printable uuid
 */
std::string BytesUuidToString(const std::string &bytesUuid);

/**
 * @brief Convert a 36-byte printable uuid to a 16-byte uuid .
 * @param[in] stringUuid A 36-byte printable uuid.
 * @param[out] byteUuid A 16-byte uuid.
 * @return Status of the call.
 */
Status StringUuidToBytes(const std::string &stringUuid, std::string &byteUuid);

/**
 * @brief Convert a uint64_t index to a 36-byte string uuid .
 * @param[in] uuidNumber A uint64_t Index.
 * @param[out] stringUuid A 36-byte string uuid.
 * @return Status of the call.
 */
Status IndexUuidGenerator(const uint64_t uuidNumber, std::string &stringUuid);

/**
 * @brief Get a 16-byte uuid.
 * @return Return a 16-byte uuid.
 */
std::string GetBytesUuid();

/**
 * @brief Get a 36-byte printable uuid.
 * @return Return a 36-byte printable uuid.
 */
std::string GetStringUuid();
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_UTIL_UUID_GENERATOR_H