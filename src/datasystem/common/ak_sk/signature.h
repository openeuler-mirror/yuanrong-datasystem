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
#ifndef DATASYSTEM_COMMON_AK_SK_SIGNATURE_H
#define DATASYSTEM_COMMON_AK_SK_SIGNATURE_H

#include <chrono>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include <openssl/hmac.h>
#include <openssl/sha.h>

#ifdef WITH_TESTS
#include "datasystem/common/inject/inject_point.h"
#endif
#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/rpc/zmq/zmq_message.h"
#include "datasystem/utils/sensitive_value.h"

namespace datasystem {

enum class AkSkType { CLIENT, SYSTEM, TENANT, OBS, UNKNOWN };

struct AkSkData {
    std::string accessKey;
    SensitiveValue secretKey;
    SensitiveValue dataKey;
    AkSkType type = AkSkType::UNKNOWN;

    bool NotComplete()
    {
        return accessKey.empty() || secretKey.Empty();
    }
};

class Signature {
public:
    Signature() = default;

    /**
     * @brief Used to do AK/SK authenticate.
     * @param[in] clientAccessKey The access key used by AK/SK authentication.
     * @param[in] clientSecretKey The secret key used by AK/SK authorize.
     */
    Signature(const std::string &clientAccessKey, const SensitiveValue &clientSecretKey);

    virtual ~Signature() = default;

    /**
     * @brief Init access key and secret key for the client to generate signature.
     * @param[in] accessKey The access key used by AK/SK authentication.
     * @param[in] secretKey The secret key used by AK/SK authorize.
     * @return Status of the call.
     */
    Status SetClientAkSk(const std::string &accessKey, SensitiveValue secretKey);

    /**
     * @brief Generate signature and insert it to req.
     * @param[in] req The request to be generate signature.
     * @return Status of the call.
     */
    template <typename ReqType>
    Status GenerateSignature(ReqType &req)
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        if (clientKey_.secretKey.Empty() || clientKey_.accessKey.empty()) {
            return Status::OK();
        }
        if (req.signature().size() > 0 || req.access_key().size() > 0) {
            req.clear_signature();
            req.clear_access_key();
        }
        req.set_timestamp(GetSystemClockTimeStampUs());
#ifdef WITH_TESTS
        INJECT_POINT("AkSk.SetTimestamp", [&req](uint64_t timestamp) {
            req.set_timestamp(timestamp);
            return Status::OK();
        });
#endif
        std::string canonicalRequest = req.SerializeAsString();
        std::string signature;
        RETURN_IF_NOT_OK(GetSignature(clientKey_, canonicalRequest.c_str(), canonicalRequest.size(), signature));
        req.set_signature(signature);
        req.set_access_key(clientKey_.accessKey);

        // New version of AK/SK generation method.
        canonicalRequest = req.SerializeAsString();
        std::string serializedSignature;
        RETURN_IF_NOT_OK(
            GetSignature(clientKey_, canonicalRequest.c_str(), canonicalRequest.size(), serializedSignature));
        g_ReqAk = clientKey_.accessKey;
        g_ReqSignature = std::move(serializedSignature);
        g_ReqTimestamp = req.timestamp();
        ZmqMessage emptyMsg;
        g_SerializedMessage= std::move(emptyMsg);
        return Status::OK();
    }

    /**
     * @brief Generate signature for stringToSign.
     * @param[in] stringToSign The string to sign.
     * @param[out] signature The signature.
     * @return Status of the call.
     */
    Status GenerateSignature(const std::string &stringToSign, std::string &signature)
    {
        CHECK_FAIL_RETURN_STATUS(!clientKey_.secretKey.Empty(), K_INVALID, "secretKey is empty.");
        Hasher hasher;
        return hasher.GetHMACSha256Hex(clientKey_.secretKey, stringToSign, signature);
    }

    /**
     * @brief Get the Access Key.
     * @return std::string The access key.
     */
    std::string GetAccessKey()
    {
        return clientKey_.accessKey;
    }

    std::string ToString() const
    {
        if (clientKey_.accessKey.empty()) {
            return "";
        }
        std::hash<std::string> hasher;
        return FormatString("ak hash: %s, sk hash: %s", hasher(clientKey_.accessKey),
                            GetTruncatedStr(std::to_string(hasher(clientKey_.secretKey.GetData()))));
    }

protected:
    /**
     * @brief copy ak sk to data.
     * @param[in] accessKey ak info
     * @param[in] secretKey sk info
     * @param[in] type the type of ak/sk
     * @param[out] data copy destination
     * @return Status of the call.
     */
    virtual Status CopyAkSk(const std::string &accessKey, SensitiveValue secretKey, AkSkType type, AkSkData &data);

    /**
     * @brief get the signature sign by sk.
     * @param[in] data ak/sk info.
     * @param[in] canonicalRequest request string.
     * @param[out] signature sign string.
     * @return Status of the call.
     */
    Status GetSignature(const AkSkData &data, const char *canonicalStr, size_t canonicalSize, std::string &signature);

    /**
     * @brief Hmac and encode signatureData.
     * @param[in] data ak/sk info.
     * @param[in] signatureData The signature data.
     * @param[in] signatureDataSize The signature data size.
     * @param[out] signature sign string.
     * @return Status of the call.
     */
    Status HmacAndEncode(const AkSkData &data, std::unique_ptr<unsigned char[]> &signatureData,
                         unsigned int signatureDataSize, std::string &signature);

    inline std::time_t GetSystemClockTimeStampUs()
    {
        // Attention: System clock is not monotonic.
        return std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now())
            .time_since_epoch()
            .count();
    }

    AkSkData clientKey_;
    mutable std::shared_timed_mutex mutex_;  // protect  clientKey_ and serverKeyMap_
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_AK_SK_SIGNATURE_H
