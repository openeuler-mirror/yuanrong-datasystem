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
 * [Experimental Feature / Work-in-Progress]
 * This module is an early implementation of Multi-Tenancy capabilities,
 * intended for future feature enablement.
 *
 * Warning: The implementation is incomplete; both APIs and data structures
 * are subject to incompatible changes in upcoming releases.
 * Do NOT use in production environments.
 *
 * Status: Under active development
 * Planned stable release: v2.0
 */
#ifndef DATASYSTEM_COMMON_AK_SK_MANAGER_H
#define DATASYSTEM_COMMON_AK_SK_MANAGER_H

#include <chrono>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include <openssl/hmac.h>
#include <openssl/sha.h>

#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/log/log.h"
#include "datasystem/utils/sensitive_value.h"

namespace datasystem {
class AkSkManager : public Signature {
public:
    /**
     * @brief Used to encode the input string.
     * @param[in] requestExpireTimeSec When AK/SK authentication is used, if the duration from the client to the server
     * is longer than the value of this parameter, the authentication fails and the service is denied.
     */
    explicit AkSkManager(const uint32_t requestExpireTimeSec = 300);

    ~AkSkManager() = default;

    /**
     * @brief Init access key and secret key for the client to generate signature.
     * @param[in] accessKey The access key used by AK/SK authentication.
     * @param[in] secretKey The secret key used by AK/SK authorize.
     * @param[in] dataKey The data key used by client to encrypt the secret key.
     * @return Status of the call.
     */
    Status SetClientAkSk(const std::string &accessKey, SensitiveValue secretKey,
                         SensitiveValue dataKey = SensitiveValue());

    /**
     * @brief Init access key and secret key for the server to verify signature.
     * @param[in] type The server AK/SK type.
     * @param[in] accessKey The access key used by AK/SK authentication.
     * @param[in] secretKey The secret key used by AK/SK authorize.
     * @param[in] dataKey The data key used by sevrer to decrypt the secret key.
     * @return Status of the call.
     */
    Status SetServerAkSk(AkSkType type, const std::string &accessKey, SensitiveValue secretKey,
                         SensitiveValue dataKey = SensitiveValue());

    /**
     * @brief Construct and use dataKey to decrypt the secret key.
     * @param[in] accessKey The access key used by AK/SK authentication.
     * @param[in] secretKey The secret key need to be decrypted and used by AK/SK authorize.
     * @return Status of the call.
     */
    Status SetTenantAkSk(const std::string &accessKey, SensitiveValue &secretKey);

    /**
     * @brief Verify signature according to req.
     * @param[in] req The request received from client.
     * @return Status of the call.
     */
    template <typename ReqType>
    Status VerifySignatureAndTimestamp(ReqType req)
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        if (serverKeyMap_.empty()) {
            return Status::OK();
        }
        std::string inputAccessKey = req.access_key();
        auto iter = serverKeyMap_.find(inputAccessKey);
        if (iter == serverKeyMap_.end()) {
            // For the security, the specific cause of the authentication failure cannot be returned to client.
            LOG(ERROR) << "The AK/SK authorized failed, the access key of req is not found.";
            RETURN_STATUS(K_NOT_AUTHORIZED, "The AK/SK authorized failed.");
        }
        if (!g_ReqAk.empty() && !g_ReqSignature.empty() && !g_SerializedMessage.Empty()) {
            auto signature = std::move(g_ReqSignature);
            auto accessKey = std::move(g_ReqAk);
            auto timestamp = g_ReqTimestamp;
            ZmqMessage serializedStr = std::move(g_SerializedMessage);
            return VerifySignatureAndTimestamp(signature, timestamp, accessKey,
                                               static_cast<const char *>(serializedStr.Data()), serializedStr.Size());
        }
        std::string inputSignature = req.signature();
        uint64_t currentTimeUs = static_cast<uint64_t>(GetSystemClockTimeStampUs());
        uint64_t inputTimestamp = req.timestamp();
        req.clear_signature();
        req.clear_access_key();
        std::string canonicalRequest = req.SerializeAsString();
        std::string calcSignature;
        RETURN_IF_NOT_OK(GetSignature(iter->second, canonicalRequest.c_str(), canonicalRequest.size(), calcSignature));
        if (inputSignature != calcSignature) {
            // For the security, the specific cause of the authentication failure cannot be returned to client.
            LOG(ERROR) << "The AK/SK authorized failed for signature inconsistency.";
            RETURN_STATUS(K_NOT_AUTHORIZED, "The AK/SK authorized failed.");
        }
        uint64_t timeDiff =
            currentTimeUs > inputTimestamp ? currentTimeUs - inputTimestamp : inputTimestamp - currentTimeUs;
        if (timeDiff > static_cast<uint64_t>(requestExpireTimeSec_) * SECOND_TO_US) {
            // For the security, the specific cause of the authentication failure cannot be returned to client.
            LOG(ERROR) << "The AK/SK authorized timeout.";
            RETURN_STATUS(K_NOT_AUTHORIZED, "The AK/SK authorized failed.");
        }
        return Status::OK();
    }

    /**
     * @brief Verify signature according to req.
     * @param[in] signature Request signature.
     * @param[in] timestamp Request timestamp.
     * @param[in] accessKey Access key.
     * @param[in] canonicalStr Serialized request memory address.
     * @param[in] canonicalSize Serialized request size.
     */
    Status VerifySignatureAndTimestamp(const std::string &signature, uint64_t timestamp, const std::string &accessKey,
                                       const char *canonicalStr, size_t canonicalSize);

    bool SystemAuthEnabled() const
    {
        return systemAuthEnabled_;
    }

    bool IsTenantAk(const std::string &accessKey) const
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        auto iter = serverKeyMap_.find(accessKey);
        if (iter == serverKeyMap_.end()) {
            return true;
        }
        return iter->second.type == AkSkType::TENANT;
    }

    bool RemoveAccessKey(const std::string &accessKey)
    {
        std::lock_guard<std::shared_timed_mutex> lock(mutex_);
        return serverKeyMap_.erase(accessKey);
    }

protected:
    Status CopyAkSk(const std::string &accessKey, SensitiveValue secretKey, AkSkType type, AkSkData &data) override;
    Status CopyDk(SensitiveValue dataKey, AkSkData &data);

private:
    bool systemAuthEnabled_{ false };
    const uint32_t SECOND_TO_US = 1000000;
    const uint32_t requestExpireTimeSec_;
    std::unordered_map<std::string, AkSkData> serverKeyMap_;
};
}  // namespace datasystem
#endif