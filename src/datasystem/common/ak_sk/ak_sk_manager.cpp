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
 * Description: The Ak/Sk manager.
 */
#include "datasystem/common/ak_sk/ak_sk_manager.h"

#include "datasystem/common/aes/aes_impl.h"
#include "datasystem/common/encrypt/secret_manager.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/raii.h"

namespace datasystem {

Status AkSkManager::VerifySignatureAndTimestamp(const std::string &signature, uint64_t timestamp,
                                                const std::string &accessKey, const char *canonicalStr,
                                                size_t canonicalSize)
{
    auto iter = serverKeyMap_.find(accessKey);
    if (iter == serverKeyMap_.end()) {
        // For the security, the specific cause of the authentication failure cannot be returned to client.
        LOG(ERROR) << "The AK/SK authorized failed, the access key of req is not found.";
        RETURN_STATUS(K_NOT_AUTHORIZED, "The AK/SK authorized failed.");
    }
    uint64_t currentTimeUs = static_cast<uint64_t>(GetSystemClockTimeStampUs());
    std::string calcSignature;
    RETURN_IF_NOT_OK(GetSignature(iter->second, canonicalStr, canonicalSize, calcSignature));
    if (signature != calcSignature) {
        // For the security, the specific cause of the authentication failure cannot be returned to client.
        LOG(ERROR) << "The AK/SK authorized failed for signature inconsistency";
        RETURN_STATUS(K_NOT_AUTHORIZED, "The AK/SK authorized failed.");
    }
    uint64_t timeDiff = currentTimeUs > timestamp ? currentTimeUs - timestamp : timestamp - currentTimeUs;
    if (timeDiff > static_cast<uint64_t>(requestExpireTimeSec_) * SECOND_TO_US) {
        // For the security, the specific cause of the authentication failure cannot be returned to client.
        LOG(ERROR) << "The AK/SK authorized timeout.";
        RETURN_STATUS(K_NOT_AUTHORIZED, "The AK/SK authorized failed.");
    }
    return Status::OK();
}

Status AkSkManager::CopyAkSk(const std::string &accessKey, SensitiveValue secretKey, AkSkType type, AkSkData &data)
{
    data.accessKey = accessKey;

    if (SecretManager::Instance()->IsRootKeyActive()) {
        std::unique_ptr<char[]> plainAk;
        int plainAkSize;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SecretManager::Instance()->Decrypt(accessKey, plainAk, plainAkSize),
                                         "AccessKey decrypt failed.");
        data.accessKey = std::string(plainAk.get(), plainAkSize);

        std::unique_ptr<char[]> plainSk;
        int plainSkSize;
        std::string cipherSk(secretKey.GetData(), secretKey.GetSize());
        Raii raii([&cipherSk] { std::fill(cipherSk.begin(), cipherSk.end(), 0); });
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SecretManager::Instance()->Decrypt(cipherSk, plainSk, plainSkSize),
                                         "SecretKey decrypt failed.");
        data.secretKey = SensitiveValue(std::move(plainSk), plainSkSize);
    } else {
        data.secretKey = std::move(secretKey);
    }

    data.type = type;
    return Status::OK();
}

Status AkSkManager::DecodeEncryptData(std::string &cipherText)
{
    // iv(12):content+tag(16) -> iv(12):tag(16):content
    auto strs = Split(cipherText, ":");
    const auto splitLen = 2;
    if (strs.size() != splitLen) {
        LOG(ERROR) << "Failed to decode cipherText, invalid strs size:" << strs.size();
        RETURN_STATUS(K_INVALID, "Invalid cipherText format");
    }
    const auto hexTagLen = AES_TAG_LEN * BYTE_PRE_HEX;
    if (strs[1].size() < hexTagLen + 1) {
        LOG(ERROR) << "Failed to decode cipherText, invalid size: " << strs[1].size();
        RETURN_STATUS(K_INVALID, "Invalid cipherText size");
    }
    auto hexContent = strs[1].substr(0, strs[1].size() - hexTagLen);
    auto contentSize = hexContent.size() / BYTE_PRE_HEX;
    auto byteContent = std::make_unique<char[]>(contentSize + 1);
    RETURN_IF_NOT_OK(
        AesImpl::DecodeToString(hexContent.c_str(), hexContent.size(), byteContent.get(), contentSize + 1));
    cipherText =
        strs[0] + ":" + strs[1].substr(strs[1].size() - hexTagLen) + ":" + std::string(byteContent.get(), contentSize);
    return Status::OK();
}

Status AkSkManager::ConstructAesAndDecrypt(SensitiveValue &encryptData)
{
    SensitiveValue systemDataKey;
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        systemDataKey = clientKey_.dataKey;
    }

    if (!systemDataKey.Empty()) {
        SensitiveValue plainText;
        auto aes = std::make_unique<AesImpl>(systemDataKey, AesImpl::Algorithm::AES_256_GCM);
        std::string cipherText(encryptData.GetData(), encryptData.GetSize());
        RETURN_IF_NOT_OK(DecodeEncryptData(cipherText));
        Raii raii([&cipherText] { std::fill(cipherText.begin(), cipherText.end(), 0); });
        RETURN_IF_NOT_OK(aes->Decrypt(cipherText, plainText));
        encryptData = std::move(plainText);
    }
    return Status::OK();
}

Status AkSkManager::SetTenantAkSk(const std::string &accessKey, SensitiveValue &secretKey)
{
    RETURN_IF_NOT_OK(ConstructAesAndDecrypt(secretKey));

    INJECT_POINT("TestAES", [] { return Status::OK(); });
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    AkSkData data;
    data.accessKey = accessKey;
    data.secretKey = std::move(secretKey);
    data.type = AkSkType::TENANT;
    serverKeyMap_[data.accessKey] = std::move(data);
    return Status::OK();
}

AkSkManager::AkSkManager(const uint32_t requestExpireTimeSec) : Signature(), requestExpireTimeSec_(requestExpireTimeSec)
{
}

Status AkSkManager::CopyDk(SensitiveValue dataKey, AkSkData &data)
{
    if (SecretManager::Instance()->IsRootKeyActive()) {
        std::unique_ptr<char[]> plainDk;
        int plainDkSize = 0;
        std::string cipherDk(dataKey.GetData(), dataKey.GetSize());
        Raii raii([&cipherDk, &plainDk, &plainDkSize] {
            std::fill(cipherDk.begin(), cipherDk.end(), 0);
            (void)memset_s(plainDk.get(), plainDkSize, 0, plainDkSize);
        });
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SecretManager::Instance()->Decrypt(cipherDk, plainDk, plainDkSize),
                                         "DataKey decrypt failed.");
        const auto decodeDkSize = static_cast<uint32_t>(plainDkSize) / BYTE_PRE_HEX;
        auto decodeDk = std::make_unique<char[]>(decodeDkSize + 1);
        RETURN_IF_NOT_OK(AesImpl::DecodeToString(plainDk.get(), plainDkSize, decodeDk.get(), decodeDkSize + 1, true));
        data.dataKey = SensitiveValue(std::move(decodeDk), decodeDkSize);
    } else {
        data.dataKey = std::move(dataKey);
    }
    return Status::OK();
}

Status AkSkManager::SetClientAkSk(const std::string &accessKey, SensitiveValue secretKey, SensitiveValue dataKey)
{
    if (accessKey.empty() || secretKey.Empty()) {
        return Status::OK();
    }
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    if (!dataKey.Empty()) {
        RETURN_IF_NOT_OK(CopyDk(std::move(dataKey), clientKey_));
    }
    return CopyAkSk(accessKey, std::move(secretKey), AkSkType::CLIENT, clientKey_);
}

Status AkSkManager::SetServerAkSk(AkSkType type, const std::string &accessKey, SensitiveValue secretKey,
                                  SensitiveValue dataKey)
{
    if (accessKey.empty() || secretKey.Empty() || type == AkSkType::CLIENT) {
        return Status::OK();
    }
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);

    AkSkData data;
    if (type == AkSkType::SYSTEM) {
        systemAuthEnabled_ = true;
    }
    RETURN_IF_NOT_OK(CopyAkSk(accessKey, std::move(secretKey), type, data));
    if (!dataKey.Empty()) {
        RETURN_IF_NOT_OK(CopyDk(std::move(dataKey), data));
    }
    serverKeyMap_[data.accessKey] = std::move(data);
    return Status::OK();
}
}  // namespace datasystem
