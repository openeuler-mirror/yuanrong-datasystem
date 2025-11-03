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
 * Description: a dummy encrypt kit implement, means no encrypt
 */
#include "datasystem/common/encrypt/phrase_pem_tls.h"

#include "securec.h"

#include "datasystem/common/log/log.h"
#include "datasystem/common/encrypt/encrypt_kit.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/ssl_authorization.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
Status PhrasePEMTLS::ReadTlsFile(const std::string &path, SensitiveValue &value)
{
    std::string textStr;
    CHECK_FAIL_RETURN_STATUS(!path.empty(), K_INVALID, "tls path is empty");
    RETURN_IF_NOT_OK(ReadFileToString(path, textStr));
    Raii cleanStr([&textStr] {
        while (!textStr.empty()) {
            textStr.pop_back();
        }
    });

    value = SensitiveValue(textStr);
    return Status::OK();
}

Status PhrasePEMTLS::GetTlsInfo(TlsConfig &config, SensitiveValue &passPhrase, TlsInfo &info)
{
    SensitiveValue ca;
    SensitiveValue cert;
    SensitiveValue key;
    RETURN_IF_NOT_OK_APPEND_MSG(ReadTlsFile(config.caPath, ca), "read tls ca file failed");
    RETURN_IF_NOT_OK_APPEND_MSG(ReadTlsFile(config.certPath, cert), "read tls cert file failed");
    if (!passPhrase.Empty()) {
        RETURN_IF_NOT_OK(DecryptRSAPrivateKeyToMemoryInPemFormat(config.keyPath, passPhrase, key));
    } else {
        RETURN_IF_NOT_OK(PhrasePass(config.keyPath, key));
    }
    info.ca = std::move(ca);
    info.cert = std::move(cert);
    info.key = std::move(key);
    return Status::OK();
}

Status PhrasePEMTLS::PhrasePass(const std::string &passPhrasePath, SensitiveValue &value)
{
    if (passPhrasePath.empty()) {
        LOG(INFO) << "Pass is empty, no need phrase";
        return Status::OK();
    }
    SensitiveValue passPhrase;
    RETURN_IF_NOT_OK_APPEND_MSG(ReadTlsFile(passPhrasePath, passPhrase), "read phrase file failed");
    std::unique_ptr<char[]> text;
    int textSize;
    RETURN_IF_NOT_OK(encryptService_->Decrypt(passPhrase.GetData(), text, textSize));
    value = SensitiveValue(std::move(text), textSize);
    return Status::OK();
}
}  // namespace datasystem