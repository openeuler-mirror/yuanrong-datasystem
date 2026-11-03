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

#include "datasystem/common/encrypt/secret_manager.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/raii.h"

namespace datasystem {

Status AkSkManager::VerifySignatureFromDigest(const std::string &signature, uint64_t timestamp,
                                              const std::string &accessKey, const AkSkDigest &digest)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    const auto key = serverKeyMap_.find(accessKey);
    CHECK_FAIL_RETURN_STATUS(key != serverKeyMap_.end(), K_NOT_AUTHORIZED, "The AK/SK authorized failed.");
    std::string expected;
    RETURN_IF_NOT_OK(GetSignatureFromDigest(key->second, digest, expected));
    CHECK_FAIL_RETURN_STATUS(signature == expected, K_NOT_AUTHORIZED, "The AK/SK authorized failed.");
    const auto now = static_cast<uint64_t>(GetSystemClockTimeStampUs());
    const auto difference = now > timestamp ? now - timestamp : timestamp - now;
    CHECK_FAIL_RETURN_STATUS(difference <= static_cast<uint64_t>(requestExpireTimeSec_) * SECOND_TO_US,
                             K_NOT_AUTHORIZED, "The AK/SK authorized failed.");
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

Status AkSkManager::SetTenantAkSk(const std::string &accessKey, SensitiveValue &secretKey)
{
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
    data.dataKey = std::move(dataKey);
    return Status::OK();
}

Status AkSkManager::SetClientAkSk(const std::string &accessKey, SensitiveValue secretKey, SensitiveValue dataKey)
{
    if (accessKey.empty() || secretKey.Empty()) {
        return Status::OK();
    }
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    AkSkData next;
    next.dataKey = clientKey_.dataKey;
    CHECK_FAIL_RETURN_STATUS(next.dataKey.GetSize() == clientKey_.dataKey.GetSize(), K_OUT_OF_MEMORY,
                             "Failed to retain client key data.");
    if (!dataKey.Empty()) {
        RETURN_IF_NOT_OK(CopyDk(std::move(dataKey), next));
    }
    RETURN_IF_NOT_OK(CopyAkSk(accessKey, std::move(secretKey), AkSkType::CLIENT, next));
    clientKey_ = std::move(next);
    return Status::OK();
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
