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
 * Description: encrypt management by scc
 */

#ifndef DATASYSTEM_COMMON_ENCRYPT_PHRASE_PEM_TLS_H
#define DATASYSTEM_COMMON_ENCRYPT_PHRASE_PEM_TLS_H

#include "datasystem/common/encrypt/encrypt_kit.h"
#include "datasystem/common/encrypt/iphrase_tls.h"

namespace datasystem {
class PhrasePEMTLS : IPhraseTLS {
public:
    PhrasePEMTLS(std::shared_ptr<EncryptKit> encryptService) : IPhraseTLS(encryptService){};

    ~PhrasePEMTLS() override = default;

    Status PhrasePass(const std::string &path, SensitiveValue &value) override;

    Status GetTlsInfo(TlsConfig &config, SensitiveValue &passPhrase, TlsInfo &info) override;

private:
    /**
     * @brief Reading a plaintext or ciphertext certificate.
     * @param[in] path Indicates the certificate path to be read.
     * @param[in] isTextEncrypted Check whether the certificate text is encrypted.
     * @param[out] value Certificate characters.
     * @return Status of the call.
     */
    Status ReadTlsFile(const std::string &path, SensitiveValue &value);
};
}  // namespace datasystem
#endif