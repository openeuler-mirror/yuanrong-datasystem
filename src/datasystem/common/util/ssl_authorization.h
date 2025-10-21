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
 * Description: SSL Auth
 */
#ifndef DATASYSTEM_COMMON_SSL_AUTHORIZATION_H
#define DATASYSTEM_COMMON_SSL_AUTHORIZATION_H

#include <memory>

#include <openssl/ossl_typ.h>
#include <openssl/ssl.h>

#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"
#include "datasystem/common/log/log.h"

namespace datasystem {
struct CTXFree {
    void operator()(SSL_CTX *p)
    {
        SSL_CTX_free(p);
    }
};


/**
 * @brief Load key and cert from memory.
 * @param[out] context SSL context.
 * @param[in] certBuffer Cert buffer from memory.
 * @param[in] certSize Cert buffer size
 * @param[in] keyBuffer Key buffer from memory.
 * @param[in]  keySize Key buffer size
 * @return Return true if success.
 */
bool LoadKeyAndCertFromMemory(SSL_CTX *context, const char *certBuffer, size_t certSize, const char *keyBuffer,
                              size_t keySize);

/**
 * @brief Load ca from memory.
 * @param[in, out] context SSL context.
 * @param[in] caBuffer ca buffer from memory.
 * @return Return true if success.
 */
bool LoadCaFromMemory(SSL_CTX *context, const char *caBuffer, size_t caSize);

/**
 * @brief Get the Ssl Error Message.
 * @return Err string.
 */
std::string GetSslErrorMessage();

/**
 * @brief Load ssl strings for err msg.
 */
void LoadSslStrings();

/**
 * @brief Parse pkcs12 file to get cert, key and CA
 * @param[in] p12Path the path of pkcs12 file
 * @param[in] passphrase the passphrase
 * @param[out] ca the CA
 * @param[out] cert the cert
 * @param[out] key the private key
 * @return Status of the call.
 */
Status ParsePKCS12(const std::string &p12Path, const SensitiveValue &passphrase, std::string &ca, SensitiveValue &cert,
                   SensitiveValue &key);

/**
 * @brief Decrypt the RSA private key to memory in pem format.
 * @param[in] keyPath The path of ciphertext RSA private key file.
 * @param[in] passphrase The passphrase.
 * @param[out] key The plaintext RSA private key in PEM format.
 * @return Status of the call.
 */
Status DecryptRSAPrivateKeyToMemoryInPemFormat(const std::string &keyPath, const SensitiveValue &passphrase,
                                               SensitiveValue &key);
}  // namespace datasystem
#endif