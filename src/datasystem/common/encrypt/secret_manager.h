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
 * Description: Encrypt or decrypt cipher.
 */
#ifndef DATASYSTEM_COMMON_ENCRYPT_SECRET_MANAGER_H
#define DATASYSTEM_COMMON_ENCRYPT_SECRET_MANAGER_H

#include <memory>
#include <mutex>

#include "datasystem/common/log/log.h"
#include "datasystem/common/encrypt/encrypt_kit.h"
#include "datasystem/common/encrypt/iphrase_tls.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/sensitive_value.h"

namespace datasystem {
Status LoadAllSecretKeys();

class SecretManager {
public:
    /**
     * @brief Singleton mode, obtaining instance.
     * @return Pointer of Allocator.
     */
    static SecretManager *Instance();

    SecretManager();

    ~SecretManager() noexcept;

    /**
     * @brief Load all the key components in secretKey.
     * @return K_OK when success, K_RUNTIME_ERROR when load one of components failed.
     */
    Status LoadSecretKeys();

    /**
     * @brief Generate rootkey by four key components in secretKey_.
     * @return K_OK when success, K_RUNTIME_ERROR when key component is out of bound or generate rootkey failed.
     */
    Status GenerateRootKey();

    /**
     * @brief Encrypts plaintext into ciphertext.
     *        ciphertext format is "iv : secret+tag". For example, iv is "1a2cge", secret is "12dcfe", tag is "93adcb",
     *        and ciphertext is "1a2cge:12dcfe93adcb".
     *        Needs to call 'GenerateRootKey()' before using for first time.
     * @param[in] plaintText Plaintext that needs to encrypt.
     * @param[out] cipher The ciphertext after encrypting plaintext.
     * @return K_OK when success, K_RUNTIME_ERROR when generates iv or tag failed, encrypt failed;
     *         K_INVALID when plaintext is out of bound.
     */
    Status Encrypt(const std::string &plaintText, std::string &cipher);

    /**
     * @brief Decrypts ciphertext into plaintext.
     *        ciphertext format is "iv : secret+tag". For example, iv is "1a2cge", secret is "12dcfe", tag is "93adcb",
     *        and ciphertext is "1a2cge:12dcfe93adcb"
     *        Needs to call 'GenerateRootKey()' before using for first time.
     * @param[in] cipher The ciphertext needs to decrypt.
     * @param[out] plainText The plaintext after decrypting ciphertext.
     * @param[out] outSize The length of plaintext.
     * @return K_OK when success, K_RUNTIME_ERROR when rootkey is out of bound, the format of ciphertext is wrong or
     *         Decrypt failed.
     */
    Status Decrypt(const std::string &cipher, std::unique_ptr<char[]> &plainText, int &outSize);

    /**
     * @brief Destroy all the key component after generating rootkey.
     * @return K_OK when success, K_RUNTIME_ERROR when destroy key component failed.
     */
    Status DestroyKeyFactor();

    /**
     * @brief Destroy all root key after authentication.
     * @return K_OK when success, K_RUNTIME_ERROR when destroy rootkey failed.
     */
    Status DestroyRootKey();

    /**
     * @brief Rand generate all of key components.
     * @param[in] k1Path Key component file path.
     * @param[in] k2Path Key component file path.
     * @param[in] k3Path Key component file path.
     * @param[in] saltPath Salt component file path.
     * @return K_OK when success, K_RUNTIME_ERROR when generate one of key components failed.
     */
    Status GenerateAllKeyComponent(const std::string &k1Path, const std::string &k2Path, const std::string &k3Path,
                                   const std::string &saltPath);

    /**
     * @brief check the encrypt service precondition.
     * @return true if satisfy precondition, otherwise false.
     */
    bool CheckPreCondition();

    /**
     * @brief Check whether the root key is activated.
     * @return bool Return true if root key is active and false if not.
     */
    bool IsRootKeyActive();

    /**
     * @brief Get CA, cert and key from sts pkcs12 or pem file.
     * @param[out] config the CA, cert, key file path.
     * @param[out] info the tls info.
     * @return Status of the call.
     */
    Status GetTlsInfo(TlsConfig &config, TlsInfo &info);

    /**
     * @brief Get CA, cert and key from pem file.
     * @param[out] config the CA, cert, key file path.
     * @param[out] info the tls info.
     * @return Status of the call.
     */
    Status GetPemTlsInfo(TlsConfig &config, TlsInfo &info);

private:
    std::shared_ptr<EncryptKit> encryptService_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_ENCRYPT_SECRET_MANAGER_H
