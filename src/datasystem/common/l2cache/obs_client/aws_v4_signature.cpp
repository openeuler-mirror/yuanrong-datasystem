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
 * Description: AWS Signature Version 4 implementation.
 */

#include "datasystem/common/l2cache/obs_client/aws_v4_signature.h"

#include <algorithm>
#include <ctime>

#include <securec.h>

#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/common/httpclient/http_request.h"

namespace datasystem {

const std::string AWS4_REQUEST = "aws4_request";
const std::string UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";
const std::string AWS4_PREFIX = "AWS4";

namespace {
std::string JoinStrings(const std::vector<std::string> &parts, const std::string &delimiter)
{
    std::string result;
    for (size_t i = 0; i < parts.size(); ++i) {
        if (i > 0) {
            result += delimiter;
        }
        result += parts[i];
    }
    return result;
}
}  // namespace

AwsV4Signature::AwsV4Signature(const std::string &region) : region_(region) {}

Status AwsV4Signature::SignRequest(
    const ObsCredential &credential, std::shared_ptr<HttpRequest> &request,
    const std::string &contentMd5, const std::map<std::string, std::string> &subResources)
{
    (void)contentMd5;
    (void)subResources;

    std::time_t now = std::time(nullptr);
    std::string amzDate = FormatDateISO8601(now);
    std::string dateStamp = FormatDateYYYYMMDD(now);
    request->AddHeader("x-amz-date", amzDate);
    request->AddHeader("x-amz-content-sha256", UNSIGNED_PAYLOAD);
    if (!credential.token.empty()) {
        request->AddHeader("x-amz-security-token", credential.token);
    }

    std::string method = HttpRequest::HttpMethodStr(request->GetMethod());
    std::string canonicalUri = ExtractCanonicalUri(request->GetUrl());
    std::string canonicalQueryString = BuildCanonicalQueryString(request->GetQueryParams());
    std::string canonicalHeaders;
    std::string signedHeaders;
    RETURN_IF_NOT_OK(BuildSignedHeaders(request->Headers(), canonicalHeaders, signedHeaders));

    std::string canonicalRequest = BuildCanonicalRequest(
        method, canonicalUri, canonicalQueryString, canonicalHeaders, signedHeaders, UNSIGNED_PAYLOAD);

    std::string credentialScope = dateStamp + "/" + region_ + "/" + service_ + "/" + AWS4_REQUEST;
    std::string stringToSign = BuildStringToSign(amzDate, credentialScope, Sha256Hex(canonicalRequest));

    std::string signingKey;
    RETURN_IF_NOT_OK(CalculateSigningKey(credential.sk, dateStamp, signingKey));
    std::string signature;
    RETURN_IF_NOT_OK(CalculateSignature(signingKey, stringToSign, signature));

    request->AddHeader("Authorization", BuildAuthorizationHeader(
        credential.ak, credentialScope, signedHeaders, signature));
    return Status::OK();
}

std::string AwsV4Signature::ExtractCanonicalUri(const std::string &url)
{
    std::string canonicalUri = "/";
    size_t pathStart = url.find("://");
    if (pathStart != std::string::npos) {
        pathStart = url.find('/', pathStart + 3);
        if (pathStart != std::string::npos) {
            size_t queryStart = url.find('?', pathStart);
            canonicalUri = (queryStart != std::string::npos)
                ? url.substr(pathStart, queryStart - pathStart)
                : url.substr(pathStart);
        }
    }
    return UriEncode(canonicalUri, false);
}

std::string AwsV4Signature::BuildCanonicalQueryString(const std::map<std::string, std::string> &queryParams)
{
    if (queryParams.empty()) {
        return "";
    }
    std::vector<std::string> sortedParams;
    sortedParams.reserve(queryParams.size());
    for (const auto &p : queryParams) {
        sortedParams.push_back(UriEncode(p.first) + "=" + UriEncode(p.second));
    }
    std::sort(sortedParams.begin(), sortedParams.end());
    return JoinStrings(sortedParams, "&");
}

Status AwsV4Signature::BuildSignedHeaders(
    const std::map<std::string, std::string> &headers, std::string &canonicalHeaders, std::string &signedHeaders)
{
    std::map<std::string, std::string> lowerHeaders;
    for (const auto &h : headers) {
        std::string lowerKey = h.first;
        std::transform(lowerKey.begin(), lowerKey.end(), lowerKey.begin(), ::tolower);
        lowerHeaders[lowerKey] = h.second;
    }

    std::vector<std::string> signedHeaderList;
    for (const auto &h : lowerHeaders) {
        if (h.first == "host" || h.first.find("x-amz-") == 0 ||
            h.first == "content-type" || h.first == "content-md5") {
            canonicalHeaders += h.first + ":" + h.second + "\n";
            signedHeaderList.push_back(h.first);
        }
    }
    signedHeaders = JoinStrings(signedHeaderList, ";");
    return Status::OK();
}

std::string AwsV4Signature::BuildCanonicalRequest(
    const std::string &method, const std::string &canonicalUri,
    const std::string &canonicalQueryString,
    const std::string &canonicalHeaders,
    const std::string &signedHeaders,
    const std::string &hashedPayload)
{
    return method + "\n" + canonicalUri + "\n" + canonicalQueryString + "\n"
        + canonicalHeaders + "\n" + signedHeaders + "\n" + hashedPayload;
}

std::string AwsV4Signature::BuildStringToSign(
    const std::string &date, const std::string &credentialScope,
    const std::string &hashedCanonicalRequest)
{
    return std::string("AWS4-HMAC-SHA256\n") + date + "\n" + credentialScope + "\n" + hashedCanonicalRequest;
}

Status AwsV4Signature::CalculateSigningKey(
    const std::string &sk, const std::string &date, std::string &signingKey)
{
    Hasher hasher;
    std::string kSecret = AWS4_PREFIX + sk;
    std::string kDate;
    RETURN_IF_NOT_OK(hasher.GetHMACSha256(kSecret, date, kDate));
    std::string kRegion;
    RETURN_IF_NOT_OK(hasher.GetHMACSha256(kDate, region_, kRegion));
    std::string kService;
    RETURN_IF_NOT_OK(hasher.GetHMACSha256(kRegion, service_, kService));
    RETURN_IF_NOT_OK(hasher.GetHMACSha256(kService, AWS4_REQUEST, signingKey));
    return Status::OK();
}

Status AwsV4Signature::CalculateSignature(
    const std::string &signingKey, const std::string &stringToSign, std::string &signature)
{
    Hasher hasher;
    return hasher.GetHMACSha256Hex(signingKey, stringToSign, signature);
}

std::string AwsV4Signature::BuildAuthorizationHeader(
    const std::string &ak, const std::string &credentialScope,
    const std::string &signedHeaders, const std::string &signature)
{
    return std::string("AWS4-HMAC-SHA256 ") +
        "Credential=" + ak + "/" + credentialScope + ", " +
        "SignedHeaders=" + signedHeaders + ", " +
        "Signature=" + signature;
}

std::string AwsV4Signature::UriEncode(const std::string &input, bool encodeSlash)
{
    std::string result;
    for (char c : input) {
        if (isalnum(c) || c == '-' || c == '.' || c == '_' || c == '~') {
            result += c;
        } else if (c == '/' && !encodeSlash) {
            result += c;
        } else {
            char buf[4];
            sprintf_s(buf, sizeof(buf), "%%%02X", static_cast<unsigned char>(c));
            result += buf;
        }
    }
    return result;
}

std::string AwsV4Signature::FormatDateISO8601(const std::time_t &now)
{
    static constexpr int ISO8601_DATE_BUF_SIZE = 20;  // YYYYMMDDTHHmmssZ + null
    std::tm gmt;
    if (gmtime_r(&now, &gmt) == nullptr) {
        return "";
    }
    char buf[ISO8601_DATE_BUF_SIZE];
    if (std::strftime(buf, sizeof(buf), "%Y%m%dT%H%M%SZ", &gmt) == 0) {
        return "";
    }
    return std::string(buf);
}

std::string AwsV4Signature::FormatDateYYYYMMDD(const std::time_t &now)
{
    static constexpr int YYYYMMDD_DATE_BUF_SIZE = 9;  // YYYYMMDD + null
    std::tm gmt;
    if (gmtime_r(&now, &gmt) == nullptr) {
        return "";
    }
    char buf[YYYYMMDD_DATE_BUF_SIZE];
    if (std::strftime(buf, sizeof(buf), "%Y%m%d", &gmt) == 0) {
        return "";
    }
    return std::string(buf);
}

std::string AwsV4Signature::Sha256Hex(const std::string &data)
{
    Hasher hasher;
    std::string hash;
    (void)hasher.GetSha256Hex(data, hash);
    return hash;
}

}  // namespace datasystem
