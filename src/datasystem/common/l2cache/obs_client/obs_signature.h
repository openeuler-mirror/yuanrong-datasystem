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
 * Description: OBS V2 signature utility for HTTP REST API.
 */

#ifndef DATASYSTEM_COMMON_L2CACHE_OBS_CLIENT_OBS_SIGNATURE_H
#define DATASYSTEM_COMMON_L2CACHE_OBS_CLIENT_OBS_SIGNATURE_H

#include <map>
#include <string>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
class ObsSignature {
public:
    ~ObsSignature() = default;
    /**
     * @brief Build OBS V2 StringToSign.
     * @param[in] method HTTP method (PUT/GET/DELETE/POST).
     * @param[in] contentMd5 MD5 of content (optional, empty if not used).
     * @param[in] contentType Content-Type header value (optional).
     * @param[in] date RFC 2616 formatted date string.
     * @param[in] obsHeaders x-obs-* headers (lowercase key, sorted).
     * @param[in] canonicalResource CanonicalizedResource string.
     * @return The constructed StringToSign.
     */
    static std::string BuildStringToSign(const std::string &method, const std::string &contentMd5,
                                         const std::string &contentType, const std::string &date,
                                         const std::map<std::string, std::string> &obsHeaders,
                                         const std::string &canonicalResource);

    /**
     * @brief Calculate OBS V2 signature (HMAC-SHA1 + Base64).
     * @param[in] sk Secret key.
     * @param[in] stringToSign The StringToSign.
     * @param[out] signature The calculated signature.
     * @return Status of the call.
     */
    static Status Sign(const std::string &sk, const std::string &stringToSign, std::string &signature);

    /**
     * @brief Build Authorization header value.
     * @param[in] ak Access key.
     * @param[in] signature The calculated signature.
     * @return "OBS {ak}:{signature}"
     */
    static std::string BuildAuthHeader(const std::string &ak, const std::string &signature);

    /**
     * @brief Build CanonicalizedResource for OBS V2 signing.
     * @param[in] bucket Bucket name.
     * @param[in] objectKey Object key (may be empty for bucket-level ops).
     * @param[in] subResources Sub-resource query params (e.g., {"uploads",""}, {"uploadId","xxx"}).
     * @return CanonicalizedResource string.
     */
    static std::string BuildCanonicalResource(const std::string &bucket, const std::string &objectKey,
                                              const std::map<std::string, std::string> &subResources = {});

    /**
     * @brief Format current time as RFC 2616 date string.
     * @return Date string like "Wed, 01 Mar 2024 12:00:00 GMT".
     */
    static std::string FormatDateRFC1123();
};
}  // namespace datasystem
#endif
