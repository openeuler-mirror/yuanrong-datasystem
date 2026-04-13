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
 * Description: OBS V2 signature utility implementation.
 */

#include "datasystem/common/l2cache/obs_client/obs_signature.h"

#include <ctime>
#include <sstream>

#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/utils/sensitive_value.h"

namespace datasystem {

std::string ObsSignature::BuildStringToSign(const std::string &method, const std::string &contentMd5,
                                            const std::string &contentType, const std::string &date,
                                            const std::map<std::string, std::string> &obsHeaders,
                                            const std::string &canonicalResource)
{
    std::ostringstream oss;
    oss << method << "\n"
        << contentMd5 << "\n"
        << contentType << "\n"
        << date << "\n";
    for (const auto &hdr : obsHeaders) {
        oss << hdr.first << ":" << hdr.second << "\n";
    }
    oss << canonicalResource;
    return oss.str();
}

Status ObsSignature::Sign(const std::string &sk, const std::string &stringToSign, std::string &signature)
{
    Hasher hasher;
    SensitiveValue skValue(sk);
    return hasher.GetHMACSha1Base64(skValue, stringToSign, signature);
}

std::string ObsSignature::BuildAuthHeader(const std::string &ak, const std::string &signature)
{
    return "OBS " + ak + ":" + signature;
}

std::string ObsSignature::BuildCanonicalResource(const std::string &bucket, const std::string &objectKey,
                                                 const std::map<std::string, std::string> &subResources)
{
    std::string resource = "/" + bucket + "/";
    if (!objectKey.empty()) {
        resource += objectKey;
    }
    if (!subResources.empty()) {
        resource += "?";
        bool first = true;
        for (const auto &sr : subResources) {
            if (!first) {
                resource += "&";
            }
            first = false;
            resource += sr.first;
            if (!sr.second.empty()) {
                resource += "=" + sr.second;
            }
        }
    }
    return resource;
}

std::string ObsSignature::FormatDateRFC1123()
{
    std::time_t now = std::time(nullptr);
    std::tm *gmt = std::gmtime(&now);
    char buf[64];
    std::strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT", gmt);
    return std::string(buf);
}

}  // namespace datasystem
