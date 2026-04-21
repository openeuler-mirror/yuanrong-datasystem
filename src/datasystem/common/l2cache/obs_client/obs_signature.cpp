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

#include <algorithm>
#include <ctime>
#include <sstream>

#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/common/httpclient/http_request.h"
#include "datasystem/utils/sensitive_value.h"

namespace datasystem {

Status ObsV2Signature::SignRequest(
    const ObsCredential &credential, std::shared_ptr<HttpRequest> &request,
    const std::string &contentMd5,
    const std::map<std::string, std::string> &subResources)
{
    // Get Date header
    std::string date = FormatDateRFC1123();
    request->AddHeader("Date", date);

    // Add security token if using temporary credentials
    if (!credential.token.empty()) {
        request->AddHeader("x-obs-security-token", credential.token);
    }

    // Build canonical resource from URL
    const std::string &url = request->GetUrl();
    std::string canonicalResource;

    // Extract bucket and object key from URL
    // URL format: http://endpoint/bucket/objectKey or http://bucket.endpoint/objectKey
    size_t pathStart = url.find("://");
    if (pathStart != std::string::npos) {
        pathStart = url.find('/', pathStart + 3);
        if (pathStart != std::string::npos) {
            size_t queryStart = url.find('?', pathStart);
            if (queryStart != std::string::npos) {
                canonicalResource = url.substr(pathStart, queryStart - pathStart);
            } else {
                canonicalResource = url.substr(pathStart);
            }
        }
    }

    // Add sub-resources to canonical resource
    if (!subResources.empty()) {
        canonicalResource += "?";
        bool first = true;
        for (const auto &sr : subResources) {
            if (!first) {
                canonicalResource += "&";
            }
            first = false;
            canonicalResource += sr.first;
            if (!sr.second.empty()) {
                canonicalResource += "=" + sr.second;
            }
        }
    }

    // Collect x-obs-* headers for signing
    std::map<std::string, std::string> obsHeaders;
    const auto &headers = request->Headers();
    for (const auto &h : headers) {
        std::string lowerKey = h.first;
        std::transform(lowerKey.begin(), lowerKey.end(), lowerKey.begin(), ::tolower);
        if (lowerKey.find("x-obs-") == 0) {
            obsHeaders[lowerKey] = h.second;
        }
    }

    // Get content-type header
    std::string contentType;
    auto ctIt = headers.find("Content-Type");
    if (ctIt == headers.end()) {
        // Try lowercase
        for (const auto &h : headers) {
            std::string lowerKey = h.first;
            std::transform(lowerKey.begin(), lowerKey.end(), lowerKey.begin(), ::tolower);
            if (lowerKey == "content-type") {
                contentType = h.second;
                break;
            }
        }
    } else {
        contentType = ctIt->second;
    }

    // Build string to sign
    std::string method = HttpRequest::HttpMethodStr(request->GetMethod());
    std::string stringToSign = BuildStringToSign(method, contentMd5, contentType, date, obsHeaders, canonicalResource);

    // Calculate signature
    std::string signature;
    RETURN_IF_NOT_OK(Sign(credential.sk, stringToSign, signature));

    // Build and set Authorization header
    std::string authHeader = BuildAuthHeader(credential.ak, signature);
    request->AddHeader("Authorization", authHeader);

    return Status::OK();
}

std::string ObsV2Signature::BuildStringToSign(
    const std::string &method, const std::string &contentMd5,
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

Status ObsV2Signature::Sign(const std::string &sk, const std::string &stringToSign, std::string &signature)
{
    Hasher hasher;
    SensitiveValue skValue(sk);
    return hasher.GetHMACSha1Base64(skValue, stringToSign, signature);
}

std::string ObsV2Signature::BuildAuthHeader(const std::string &ak, const std::string &signature)
{
    return "OBS " + ak + ":" + signature;
}

std::string ObsV2Signature::BuildCanonicalResource(
    const std::string &bucket, const std::string &objectKey,
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

std::string ObsV2Signature::FormatDateRFC1123()
{
    static constexpr int RFC1123_DATE_BUF_SIZE = 64;
    std::time_t now = std::time(nullptr);
    std::tm gmt;
    if (gmtime_r(&now, &gmt) == nullptr) {
        return "";
    }
    char buf[RFC1123_DATE_BUF_SIZE];
    if (std::strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT", &gmt) == 0) {
        return "";
    }
    return std::string(buf);
}

}  // namespace datasystem
