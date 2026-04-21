/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: Abstract signature provider interface for OBS/S3 authentication.
 */

#ifndef DATASYSTEM_COMMON_L2CACHE_OBS_CLIENT_OBS_SIGNATURE_PROVIDER_H
#define DATASYSTEM_COMMON_L2CACHE_OBS_CLIENT_OBS_SIGNATURE_PROVIDER_H

#include <cstdint>
#include <string>
#include <map>
#include <memory>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {

// Forward declarations
class HttpRequest;

/**
 * @brief Signature type enumeration.
 */
enum class SignatureType : uint8_t {
    OBS_V2,   // Huawei OBS V2 signature (HMAC-SHA1)
    AWS_V4    // AWS S3 V4 signature (HMAC-SHA256)
};

/**
 * @brief OBS credential structure.
 */
struct ObsCredential {
    std::string ak;      // Access key
    std::string sk;      // Secret key
    std::string token;   // Security token (for temporary credentials)
};

/**
 * @brief Abstract signature provider interface.
 *
 * Implementations provide specific signature algorithms (OBS V2, AWS V4).
 */
class ObsSignatureProvider {
public:
    virtual ~ObsSignatureProvider() = default;

    /**
     * @brief Sign an HTTP request.
     * @param[in] credential Credential for signing.
     * @param[in,out] request HTTP request to sign.
     * @param[in] contentMd5 MD5 of content (optional, for OBS V2).
     * @param[in] subResources Query sub-resources for canonical resource.
     * @return Status of the call.
     */
    virtual Status SignRequest(const ObsCredential &credential,
                               std::shared_ptr<HttpRequest> &request,
                               const std::string &contentMd5,
                               const std::map<std::string, std::string> &subResources) = 0;

    /**
     * @brief Get the signature type.
     * @return Signature type.
     */
    virtual SignatureType GetType() const = 0;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_L2CACHE_OBS_CLIENT_OBS_SIGNATURE_PROVIDER_H
