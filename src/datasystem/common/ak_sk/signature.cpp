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
 * Description: generate ak/sk signature
 */
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/utils/status.h"

namespace datasystem {
Signature::Signature(const std::string &clientAccessKey, const SensitiveValue &clientSecretKey)
{
    if (clientAccessKey.empty() || clientSecretKey.Empty()) {
        return;
    }
    LOG_IF_ERROR(CopyAkSk(clientAccessKey, clientSecretKey, AkSkType::CLIENT, clientKey_), "Signature CopyAkSk failed");
}

Status Signature::SetClientAkSk(const std::string &accessKey, SensitiveValue secretKey)
{
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    return CopyAkSk(accessKey, std::move(secretKey), AkSkType::CLIENT, clientKey_);
}

Status Signature::CopyAkSk(const std::string &accessKey, SensitiveValue secretKey, AkSkType type, AkSkData &data)
{
    data.accessKey = accessKey;
    data.secretKey = std::move(secretKey);
    data.type = type;
    return Status::OK();
}

Status Signature::GetSignature(const AkSkData &data, const char *canonicalStr, size_t canonicalSize,
                               std::string &signature)
{
    CHECK_FAIL_RETURN_STATUS(!data.secretKey.Empty(), K_INVALID, "secretKey is empty.");
    Hasher hasher;
    // 1. calc hash and encode
    std::unique_ptr<unsigned char[]> outHashData;
    unsigned int outHashSize;
    RETURN_IF_NOT_OK(hasher.HashSHA256(canonicalStr, canonicalSize, outHashData, outHashSize));
    std::unique_ptr<unsigned char[]> signatureData;
    unsigned int signatureDataSize;
    RETURN_IF_NOT_OK(hasher.HexEncodeForAkSk(outHashData, outHashSize, signatureData, signatureDataSize));

    // 2. hmac and encode
    std::unique_ptr<unsigned char[]> outSignature;
    unsigned int outSignatureSize;
    RETURN_IF_NOT_OK(hasher.Hmac(data.secretKey.GetData(), data.secretKey.GetSize(), signatureData, signatureDataSize,
                                 outSignature, outSignatureSize));

    return hasher.HexEncodeForAkSk(outSignature, outSignatureSize, signature);
}
}  // namespace datasystem