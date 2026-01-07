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
 * Description: Description: Encrypt or decrypt cipher.
 */
#include "datasystem/common/encrypt/secret_manager.h"

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/pkcs12.h>
#include <openssl/x509.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/encrypt/phrase_pem_tls.h"
#include "datasystem/common/util/ssl_authorization.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/utils/sensitive_value.h"

DS_DECLARE_string(encrypt_kit);

namespace datasystem {

Status LoadAllSecretKeys()
{
    if (FLAGS_encrypt_kit == ENCRYPT_KIT_PLAINTEXT) {
        LOG(WARNING) << "The encrypt kit choose plaintext, avoid use in production environments.";
    }
    return Status::OK();
}

SecretManager *SecretManager::Instance()
{
    static SecretManager instance;
    return &instance;
}

SecretManager::SecretManager()
{
    if (FLAGS_encrypt_kit == ENCRYPT_KIT_PLAINTEXT) {
        LOG(WARNING) << "Enable plaintext encrypt_kit. Therefore, the value is directly returned.";
        encryptService_ = std::make_unique<EncryptKit>();
    }
}

SecretManager::~SecretManager() noexcept
{
    encryptService_.reset();
}

bool SecretManager::CheckPreCondition()
{
    return encryptService_->CheckPreCondition();
}

bool SecretManager::IsRootKeyActive()
{
    INJECT_POINT("SecretManager.RootKeyActive", []() {
        return true;
    });
    return encryptService_->IsActive();
}

Status SecretManager::LoadSecretKeys()
{
    return encryptService_->LoadSecretKeys();
}

Status SecretManager::GenerateRootKey()
{
    return encryptService_->GenerateRootKey();
}

Status SecretManager::DestroyKeyFactor()
{
    return encryptService_->DestroyKeyFactor();
}

Status SecretManager::DestroyRootKey()
{
    return encryptService_->DestroyRootKey();
}

Status SecretManager::Encrypt(const std::string &plaintText, std::string &cipher)
{
    return encryptService_->Encrypt(plaintText, cipher);
}

Status SecretManager::Decrypt(const std::string &cipher, std::unique_ptr<char[]> &plainText, int &outSize)
{
    return encryptService_->Decrypt(cipher, plainText, outSize);
}

Status SecretManager::GenerateAllKeyComponent(const std::string &k1Path, const std::string &k2Path,
                                              const std::string &k3Path, const std::string &saltPath)
{
    return encryptService_->GenerateAllKeyComponent(k1Path, k2Path, k3Path, saltPath);
}

Status SecretManager::GetPemTlsInfo(TlsConfig &config, TlsInfo &info)
{
    SensitiveValue passPhrase;
    auto phraseTlsService = std::make_shared<PhrasePEMTLS>(encryptService_);
    RETURN_IF_NOT_OK(phraseTlsService->PhrasePass(config.passPhrasePath, passPhrase));
    return phraseTlsService->GetTlsInfo(config, passPhrase, info);
}

Status SecretManager::GetTlsInfo(TlsConfig &config, TlsInfo &info)
{
    return GetPemTlsInfo(config, info);
}
}  // namespace datasystem