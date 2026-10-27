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

#include <algorithm>
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
    AkSkData next;
    next.dataKey = clientKey_.dataKey;
    CHECK_FAIL_RETURN_STATUS(next.dataKey.GetSize() == clientKey_.dataKey.GetSize(), K_OUT_OF_MEMORY,
                             "Failed to retain client key data.");
    RETURN_IF_NOT_OK(CopyAkSk(accessKey, std::move(secretKey), AkSkType::CLIENT, next));
    clientKey_ = std::move(next);
    return Status::OK();
}

Status Signature::SnapshotClientKey(AkSkData &out) const
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    AkSkData snapshot;
    snapshot.accessKey = clientKey_.accessKey;
    snapshot.secretKey = clientKey_.secretKey;
    CHECK_FAIL_RETURN_STATUS(snapshot.secretKey.GetSize() == clientKey_.secretKey.GetSize(), K_OUT_OF_MEMORY,
                             "Failed to snapshot client credentials.");
    snapshot.type = clientKey_.type;
    out = std::move(snapshot);
    return Status::OK();
}

Status Signature::GetSignatureFromDigest(const AkSkData &key, const AkSkDigest &digest, std::string &signature)
{
    CHECK_FAIL_RETURN_STATUS(!key.secretKey.Empty(), K_INVALID, "secretKey is empty.");
    auto hash = std::make_unique<unsigned char[]>(digest.size());
    std::copy(digest.begin(), digest.end(), hash.get());
    Hasher hasher;
    std::unique_ptr<unsigned char[]> encoded;
    unsigned int encodedSize = 0;
    RETURN_IF_NOT_OK(hasher.HexEncodeForAkSk(hash, digest.size(), encoded, encodedSize));
    return HmacAndEncode(key, encoded, encodedSize, signature);
}

Status Signature::CopyAkSk(const std::string &accessKey, SensitiveValue secretKey, AkSkType type, AkSkData &data)
{
    data.accessKey = accessKey;
    data.secretKey = std::move(secretKey);
    data.type = type;
    return Status::OK();
}

Status Signature::HmacAndEncode(const AkSkData &data, std::unique_ptr<unsigned char[]> &signatureData,
                                unsigned int signatureDataSize, std::string &signature)
{
    Hasher hasher;
    std::unique_ptr<unsigned char[]> outSignature;
    unsigned int outSignatureSize;
    RETURN_IF_NOT_OK(hasher.Hmac(data.secretKey.GetData(), data.secretKey.GetSize(), signatureData, signatureDataSize,
                                 outSignature, outSignatureSize));

    return hasher.HexEncodeForAkSk(outSignature, outSignatureSize, signature);
}
}  // namespace datasystem