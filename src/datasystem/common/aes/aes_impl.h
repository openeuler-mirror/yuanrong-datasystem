/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: The cipher of AES define.
 */

#ifndef DATASYSTEM_UTILS_AES_IMPL_H
#define DATASYSTEM_UTILS_AES_IMPL_H

#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"

namespace datasystem {
const size_t AES_IV_LEN = 12;
const size_t AES_TAG_LEN = 16;
const size_t BYTE_PRE_HEX = 2;
class AesImpl {
public:
    enum Algorithm {
        AES_128_GCM,
        AES_192_GCM,
        AES_256_GCM,
    };

    AesImpl(SensitiveValue key, Algorithm algorithm) : key_(std::move(key)), algorithm_(algorithm)
    {
    }

    /**
     * @brief Encrypt plaintext.
     * @param[in] plainText The plaintext need to encrypt.
     * @param[out] cipherText Ciphertext with aes-gcm.
     * @return K_OK when success; K_RUNTIME_ERROR otherwise.
     */
    Status Encrypt(const SensitiveValue &plainText, std::string &cipherText);

    /**
     * @brief Decrypt ciphertext.
     * @param[in] cipherText The ciphertext need to decrypt.
     * @param[out] plainText Plaintext with aes-gcm.
     * @return K_OK when success; K_RUNTIME_ERROR otherwise.
     */
    Status Decrypt(const std::string &cipherText, SensitiveValue &plainText);

    /**
     * @brief Check whether the algorithm matches the key.
     * @return K_OK when success; K_INVALID otherwise.
     */
    Status CheckKey();

    /**
     * @brief Convert bytes string to hex string.
     * @param[in] str The bytes string.
     * @return hex string.
     */
    static std::string EncodeToHexString(const std::string &str);

    /**
     * @brief Convert hex string to bytes string.
     * @param[in] str The hex string.
     * @param[in] strLen The hex string length.
     * @param[out] result The bytes string.
     * @param[out] resultLen Output result max Len.
     * @param[in] isCap Hex string is uppercase or not.
     * @note If "00" in str, may decode to '\0'.
     * @note Must initialize result before decode.
     * @return K_OK when decode to bytes string success, K_RUNTIME_ERROR otherwise.
     */
    static Status DecodeToString(const char *str, size_t strLen, char *result, size_t resultLen, bool isCap = false);

private:
    /**
     * @brief Divide the ciphertext into three parts: IV, cipher, and tag.
     * @param[in/out] cipher Origin ciphertext.
     * @param[out] ivBytes Hash distribution of keys.
     * @param[out] tagBytes Tag for specifying additional authenticated data.
     * @param[out] cipherOffset Offset of cipherBytes.
     * @return K_OK when success; K_RUNTIME_ERROR otherwise.
     */
    static Status SplitCipherStr(const std::string &cipher, std::unique_ptr<char[]> &ivBytes,
                                 std::unique_ptr<char[]> &tagBytes, size_t &cipherOffset);

    SensitiveValue key_;
    Algorithm algorithm_;
};
}  // namespace datasystem

#endif