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

#ifndef DATASYSTEM_UTILS_AES_H
#define DATASYSTEM_UTILS_AES_H

#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"

namespace datasystem {
class AesImpl;

class __attribute((visibility("default"))) Aes {
public:
    enum Algorithm {
        AES_128_GCM,
        AES_192_GCM,
        AES_256_GCM,
    };
    Aes(SensitiveValue key, Algorithm algorithm);

    /// \brief Encrypt plaintext.
    ///
    /// \param[in] plainText The plaintext need to encrypt.
    /// \param[out] cipherText Ciphertext with aes-gcm.
    ///
    /// \return K_OK when success; K_RUNTIME_ERROR otherwise.
    Status Encrypt(const SensitiveValue &plainText, std::string &cipherText);

    /// \brief Decrypt ciphertext.
    ///
    /// \param[in] cipherText The ciphertext need to decrypt.
    /// \param[out] plainText Plaintext with aes-gcm.
    ///
    /// \return K_OK when success; K_RUNTIME_ERROR otherwise.
    Status Decrypt(const std::string &cipherText, SensitiveValue &plainText);

private:
    std::shared_ptr<AesImpl> impl_;
};
}  // namespace datasystem

#endif