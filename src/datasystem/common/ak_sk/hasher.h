/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: The hash function used by ak/sk.
 */
#ifndef DATASYSTEM_COMMON_AK_SK_HASHER_H
#define DATASYSTEM_COMMON_AK_SK_HASHER_H

#include <memory>
#include <string>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/sensitive_value.h"

namespace datasystem {
class Hasher {
public:
    Hasher() = default;

    /**
     * @brief Used to encode the input string.
     * @param[in] inputData The point of input data.
     * @param[in] inputDataSize The size of input data.
     * @param[out] signature encode the input string.
     * @return Status of the call
     */
    Status HexEncode(std::unique_ptr<unsigned char[]> &inputData, unsigned int inputDataSize, std::string &signature);

    /**
     * @brief Used to encode the input string.
     * @param[in] inputData The point of input data.
     * @param[in] inputDataSize The size of input data.
     * @param[out] outData The point of output data.
     * @param[out] outSize The size of output data.
     * @return Status of the call
     */
    Status HexEncode(std::unique_ptr<unsigned char[]> &inputData, unsigned int inputDataSize,
                   std::unique_ptr<unsigned char[]> &outData, unsigned int &outSize);

    /**
     * @brief Used to encode the input string, with '\0' at the end. Only for Ak/sk for the sake of compatibility.
     * @param[in] inputData The point of input data.
     * @param[in] inputDataSize The size of input data.
     * @param[out] signature encode the input string.
     * @return Status of the call
     */
    Status HexEncodeForAkSk(std::unique_ptr<unsigned char[]> &inputData, unsigned int inputDataSize,
                            std::string &signature);

    /**
     * @brief Used to encode the input string, with '\0' at the end. Only for Ak/sk for the sake of compatibility.
     * @param[in] inputData The point of input data.
     * @param[in] inputDataSize The size of input data.
     * @param[out] outData The point of output data.
     * @param[out] outSize The size of output data.
     * @return Status of the call
     */
    Status HexEncodeForAkSk(std::unique_ptr<unsigned char[]> &inputData, unsigned int inputDataSize,
                   std::unique_ptr<unsigned char[]> &outData, unsigned int &outSize);

    /**
     * @brief Use the SHA256 algorithm to generate a hash.
     * @param[in] data The input string c_str().
     * @param[in] size The input string size.
     * @param[out] outData The point of output data.
     * @param[out] outSize The size of output data.
     * @return Status of the call
     */
    Status HashSHA256(const char *data, size_t size, std::unique_ptr<unsigned char[]> &outData, unsigned int &outSize);

    /**
     * @brief Used to encode the input string.
     * @param[in] key The secret key.
     * @param[in] keyLen The length of the key.
     * @param[in] inputData The point of input data.
     * @param[in] inputDataSize The size of input data.
     * @param[out] outData The point of output data.
     * @param[out] outSize The size of output data.
     * @return Status of the call
     */
    Status Hmac(const void *key, int keyLen, std::unique_ptr<unsigned char[]> &inputData, unsigned int inputDataSize,
              std::unique_ptr<unsigned char[]> &outData, unsigned int &outSize);

    /**
     * @brief Get HMAC Sha256.
     * @param[in] key The secret key.
     * @param[in] data The data to calculate.
     * @param[out] sha256 The sha256 value.
     * @return Status of the call.
     */
    Status GetHMACSha256(const SensitiveValue &key, const std::string &data, std::string &sha256);

    /**
     * @brief Get HMAC Sha256 Hex.
     * @param[in] key The secret key.
     * @param[in] data The data to calculate.
     * @param[out] sha256 The sha256 value with hex encode.
     * @return Status of the call.
     */
    Status GetHMACSha256Hex(const SensitiveValue &key, const std::string &data, std::string &sha256);
    /**
     * @brief Get hash256 with hex encode.
     * @param[in] str The string to hash.
     * @param[out] hashVal The hash256 with hex encode.
     * @return Status of the call.
     */
    Status GetSha256Hex(const std::string &str, std::string &hashVal);
};
}  // namespace datasystem
#endif
