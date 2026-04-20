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
 * Description: OBS/S3 service type detector implementation.
 */

#include "datasystem/common/l2cache/obs_client/obs_service_detector.h"

#include <algorithm>

#include "datasystem/common/httpclient/curl_http_client.h"
#include "datasystem/common/httpclient/http_request.h"
#include "datasystem/common/httpclient/http_response.h"
#include "datasystem/common/log/log.h"

namespace datasystem {

static constexpr int64_t DETECTION_REQUEST_TIMEOUT_MS = 5000;
static constexpr int64_t DETECTION_CONNECT_TIMEOUT_MS = 3000;
static const std::string DEFAULT_S3_REGION = "us-east-1";

SignatureType ObsServiceDetector::Detect(
    std::shared_ptr<CurlHttpClient> &httpClient,
    const std::string &endpoint,
    const std::string &bucket,
    bool httpsEnabled)
{
    LOG(INFO) << "Detecting service type for endpoint: " << endpoint;

    // Build GET request URL (use GET instead of HEAD since HEAD is not supported)
    std::string scheme = httpsEnabled ? "https://" : "http://";
    std::string url;
    if (httpsEnabled) {
        // Virtual-hosted-style
        url = scheme + bucket + "." + endpoint + "/";
    } else {
        // Path-style
        url = scheme + endpoint + "/" + bucket + "/";
    }

    auto request = std::make_shared<HttpRequest>();
    request->SetUrl(std::move(url));
    request->SetMethod(HttpMethod::GET);
    request->SetRequestTimeoutMs(DETECTION_REQUEST_TIMEOUT_MS);
    request->SetConnectTimeoutMs(DETECTION_CONNECT_TIMEOUT_MS);

    // Create response with body stream to receive response data
    auto response = std::make_shared<HttpResponse>();
    auto respStream = std::make_shared<std::stringstream>();
    response->SetBody(respStream);

    Status rc = httpClient->Send(request, response);

    if (rc.IsError()) {
        LOG(WARNING) << "Service detection failed: " << rc.ToString()
                     << ", defaulting to OBS V2 signature";
        return SignatureType::OBS_V2;
    }

    // Check response headers
    SignatureType type = CheckServiceType(response->Headers());

    LOG(INFO) << "Detected service type: "
              << (type == SignatureType::AWS_V4 ? "AWS V4" : "OBS V2");

    return type;
}

SignatureType ObsServiceDetector::CheckServiceType(
    const std::map<std::string, std::string> &headers)
{
    static const std::string OBS_KEY = "obs";
    static const std::string MINIO_KEY = "minio";
    static const std::string AMAZON_S3_KEY = "amazons3";

    // Normalize all headers to lowercase keys for case-insensitive lookup
    std::map<std::string, std::string> lowerHeaders;
    for (const auto &h : headers) {
        std::string lowerKey = h.first;
        std::transform(lowerKey.begin(), lowerKey.end(), lowerKey.begin(), ::tolower);
        lowerHeaders[lowerKey] = h.second;
    }

    // Check Server header
    auto serverIt = lowerHeaders.find("server");
    if (serverIt != lowerHeaders.end()) {
        std::string server = serverIt->second;
        std::transform(server.begin(), server.end(), server.begin(), ::tolower);

        if (server.find(OBS_KEY) != std::string::npos) {
            LOG(INFO) << "Detected Huawei OBS from Server header: " << serverIt->second;
            return SignatureType::OBS_V2;
        }

        if (server.find(MINIO_KEY) != std::string::npos) {
            LOG(INFO) << "Detected MinIO from Server header: " << serverIt->second;
            return SignatureType::AWS_V4;
        }

        if (server.find(AMAZON_S3_KEY) != std::string::npos) {
            LOG(INFO) << "Detected Amazon S3 from Server header: " << serverIt->second;
            return SignatureType::AWS_V4;
        }
    }

    // Check x-obs-request-id (OBS specific) and x-amz-request-id (S3 specific)
    if (lowerHeaders.find("x-obs-request-id") != lowerHeaders.end()) {
        LOG(INFO) << "Detected Huawei OBS from x-obs-request-id header";
        return SignatureType::OBS_V2;
    }
    if (lowerHeaders.find("x-amz-request-id") != lowerHeaders.end()) {
        LOG(INFO) << "Detected S3/MinIO from x-amz-request-id header";
        return SignatureType::AWS_V4;
    }

    // Default to OBS V2
    LOG(INFO) << "Could not determine service type, defaulting to OBS V2";
    return SignatureType::OBS_V2;
}

std::string ObsServiceDetector::ParseRegionFromEndpoint(const std::string &endpoint)
{
    // Remove port if present
    std::string host = endpoint.substr(0, endpoint.find(':'));

    // Pattern 1: obs.{region}.myhuaweicloud.com
    // Pattern 2: s3.{region}.amazonaws.com
    // Pattern 3: {bucket}.s3.{region}.amazonaws.com

    size_t dot1 = host.find('.');
    if (dot1 == std::string::npos) {
        return DEFAULT_S3_REGION;  // Default for MinIO
    }

    // Check if first segment is obs or s3
    std::string firstSegment = host.substr(0, dot1);
    std::transform(firstSegment.begin(), firstSegment.end(), firstSegment.begin(), ::tolower);

    if (firstSegment == "obs" || firstSegment == "s3") {
        // Extract region from second segment
        size_t dot2 = host.find('.', dot1 + 1);
        if (dot2 != std::string::npos) {
            std::string region = host.substr(dot1 + 1, dot2 - dot1 - 1);
            // Validate it looks like a region (contains only letters, numbers, hyphens)
            bool validRegion = true;
            for (char c : region) {
                if (!isalnum(c) && c != '-') {
                    validRegion = false;
                    break;
                }
            }
            if (validRegion && !region.empty()) {
                LOG(INFO) << "Parsed region from endpoint: " << region;
                return region;
            }
        }
    }

    // Check for bucket.s3.{region}.amazonaws.com pattern
    if (host.find(".s3.") != std::string::npos) {
        size_t s3Pos = host.find(".s3.");
        size_t regionStart = s3Pos + 4;
        size_t dotAfterRegion = host.find('.', regionStart);
        if (dotAfterRegion != std::string::npos) {
            std::string region = host.substr(regionStart, dotAfterRegion - regionStart);
            LOG(INFO) << "Parsed region from S3 endpoint: " << region;
            return region;
        }
    }

    // Default for MinIO and unknown endpoints
    LOG(INFO) << "Using default region " << DEFAULT_S3_REGION << " for endpoint: " << endpoint;
    return DEFAULT_S3_REGION;
}

}  // namespace datasystem
