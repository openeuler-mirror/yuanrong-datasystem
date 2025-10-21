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
 * Description: the interface of Encrypt or decrypt cipher.
 */
#ifndef DATASYSTEM_COMMON_ENCRYPT_ENRYPT_SERVICE_H
#define DATASYSTEM_COMMON_ENCRYPT_ENRYPT_SERVICE_H

#include <memory>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
class EncryptKit {
public:
    EncryptKit() = default;
    virtual ~EncryptKit() = default;

    /**
     * @brief Load all the key components in secretKey.
     * @return Status of the call.
     */
    virtual Status LoadSecretKeys();

    /**
     * @brief Generate rootkey by four key components in secretKey_.
     * @return Status of the call.
     */
    virtual Status GenerateRootKey();

    /**
     * @brief Decrypts ciphertext into plaintext.
     * @param[in] cipher The ciphertext needs to decrypt.
     * @param[out] plainText The plaintext after decrypting ciphertext. The caller is responsible for freeing the
     * memory.
     * @param[out] outSize The length of plaintext.
     * @return Status of the call.
     */
    virtual Status Decrypt(const std::string &cipher, std::unique_ptr<char[]> &plainText, int &outSize);

    /**
     * @brief Encrypts plaintext into ciphertext.
     * @param[in] plaintText text to encrypt
     * @param[out] cipher the plainText encrypt content. The caller is responsible for freeing the memory.
     * @return Status of the call.
     */
    virtual Status Encrypt(const std::string &plaintText, std::string &cipher);

    /**
     * @brief Destroy all the key component after generating rootkey.
     * @return Status of the call.
     */
    virtual Status DestroyKeyFactor();

    /**
     * @brief Destroy all root key after authentication.
     * @return Status of the call.
     */
    virtual Status DestroyRootKey();

    /**
     * @brief whether the encrypt config has been set
     * @return true when the encrypt config has been set, otherwise false
     */
    virtual bool CheckPreCondition();

    /**
     * @brief whether the encrypt service is able to use
     * @return true when the encrypt service is able to use, otherwise false
     */
    virtual bool IsActive();

    /**
     * @brief Rand generate all of key components.
     * @param[in] k1Path Key component file path.
     * @param[in] k2Path Key component file path.
     * @param[in] k3Path Key component file path.
     * @param[in] saltPath Salt component file path.
     * @return Status of the call.
     */
    virtual Status GenerateAllKeyComponent(const std::string &k1Path, const std::string &k2Path,
                                           const std::string &k3Path, const std::string &saltPath);
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_ENCRYPT_ENRYPT_SERVICE_H
