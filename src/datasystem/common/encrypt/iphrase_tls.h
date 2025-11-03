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

#ifndef DATASYSTEM_COMMON_ENCRYPT_PHRASE_FOR_TLS_H
#define DATASYSTEM_COMMON_ENCRYPT_PHRASE_FOR_TLS_H

#include <memory>

#include "datasystem/common/encrypt/encrypt_kit.h"
#include "datasystem/utils/sensitive_value.h"

namespace datasystem {
struct TlsInfo {
    SensitiveValue ca;
    SensitiveValue cert;
    SensitiveValue key;
};

struct TlsConfig {
    std::string caPath;
    std::string certPath;
    std::string keyPath;
    std::string pkcs12Path;
    std::string passPhrasePath;
};

class IPhraseTLS {
public:
    explicit IPhraseTLS(std::shared_ptr<EncryptKit> encryptService) : encryptService_(std::move(encryptService)){};

    virtual ~IPhraseTLS() = default;

    virtual Status PhrasePass(const std::string &path, SensitiveValue &value) = 0;

    virtual Status GetTlsInfo(TlsConfig &config, SensitiveValue &passPhrase, TlsInfo &info) = 0;

protected:
    std::shared_ptr<EncryptKit> encryptService_;
};
}  // namespace datasystem
#endif