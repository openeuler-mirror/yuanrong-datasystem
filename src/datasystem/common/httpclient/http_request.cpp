/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: http request
 */
#include "datasystem/common/httpclient/http_request.h"

#include <algorithm>
#include <sstream>

#include <re2/re2.h>

#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/utils/status.h"

namespace datasystem {
const static int32_t FIRST_FOUR_BIT_MOVE = 4;
const static std::string HEX_STRING_SET_CAP = "0123456789ABCDEF";
HttpRequest::HttpRequest() : HttpMessage(), method_(), url_(), asyncElapse_(0)
{
}

void HttpRequest::SetMethod(HttpMethod &&method)
{
    method_ = std::move(method);
}

const HttpMethod &HttpRequest::GetMethod()
{
    return method_;
}

void HttpRequest::SetUrl(std::string &&url)
{
    url_ = std::move(url);
}

const std::string &HttpRequest::GetUrl()
{
    return url_;
}

void HttpRequest::SetConnectTimeoutMs(int64_t connectTimeoutMs)
{
    connectTimeoutMs_ = connectTimeoutMs;
}

uint64_t HttpRequest::GetConnectTimeoutMs()
{
    return connectTimeoutMs_;
}

void HttpRequest::SetRequestTimeoutMs(int64_t requestTimeoutMs)
{
    requestTimeoutMs_ = requestTimeoutMs;
}

uint64_t HttpRequest::GetRequestTimeoutMs()
{
    return requestTimeoutMs_;
}

void HttpRequest::BuildRequest()
{
}

void HttpRequest::ClearSensitiveInfo()
{
    auto size = headers_["Authorization"].size();
    if (size > 0) {
        headers_["Authorization"].assign(size, '0');
    }
}

void HttpRequest::SetAsyncElapseTime(int64_t asyncElapseTime)
{
    asyncElapse_ = asyncElapseTime >= 0 ? static_cast<uint64_t>(asyncElapseTime) : 0;
}

uint64_t HttpRequest::GetAsyncElapseTime()
{
    return asyncElapse_;
}

Status HttpRequest::GetBodyHash(std::string &hashVal)
{
    std::shared_ptr<std::stringstream> ss = std::dynamic_pointer_cast<std::stringstream>(GetBody());
    std::string body;
    if (ss) {
        body = ss->str();
    }
    if (body.empty()) {
        hashVal = EMPTY_CONTENT_SHA256;
        return Status::OK();
    }
    Hasher hasher;
    return hasher.GetSha256Hex(body, hashVal);
}

bool ShouldQueryEscape(char c)
{
    if (std::isalnum(c)) {
        // A~Z, a~z, 0~9 not escape
        return false;
    }

    if (c == '-' || c == '_' || c == '.' || c == '~') {
        // -, _, ., ~ not escape
        return false;
    }

    // encode according to RFC 3986.
    return true;
}

std::string EscapeURL(const std::string &url, bool replacePath)
{
    if (url.empty()) {
        return "";
    }

    std::stringstream ss;

    for (const auto &c : url) {
        if (!ShouldQueryEscape(c)) {
            ss << c;
        } else if (' ' == c) {
            ss << '+';
        } else {
            // encode according to RFC 3986.
            ss << '%' << HEX_STRING_SET_CAP[c >> FIRST_FOUR_BIT_MOVE] << HEX_STRING_SET_CAP[c & 0xf];
        }
    }
    std::string encodeurl = ss.str();

    ReplaceRecursively(encodeurl, "+", "%20");
    ReplaceRecursively(encodeurl, "*", "%2A");

    ReplaceRecursively(encodeurl, "%7E", "~");
    if (replacePath) {
        ReplaceRecursively(encodeurl, "%2F", "/");
    }

    return encodeurl;
}

std::string HttpRequest::GetCanonicalQueryParams(bool isEscape)
{
    if (queryParams_.empty()) {
        return "";
    }

    bool first = true;
    std::stringstream ss;
    for (const auto &param : queryParams_) {
        if (!first) {
            ss << "&";
        }
        first = false;
        if (isEscape) {
            ss << EscapeURL(param.first, false) << "=" << EscapeURL(param.second, false);
        } else {
            ss << param.first << "=" << param.second;
        }
    }
    return ss.str();
}

std::string HttpRequest::GetCanonicalHeaders()
{
    std::stringstream ss;
    for (const auto &[key, val] : headers_) {
        std::string lowerKey = key;
        std::transform(lowerKey.begin(), lowerKey.end(), lowerKey.begin(), ::tolower);
        if (lowerKey == HEADER_CONNECTION || lowerKey == HEADER_AUTHORIZATION) {
            continue;
        }
        ss << lowerKey << ':' << Trim(val) << '\n';
    }
    return ss.str();
}

std::string HttpRequest::GetSignedHeaders()
{
    bool first = true;
    std::stringstream ss;
    for (const auto &header : headers_) {
        std::string lowerKey = header.first;
        std::transform(lowerKey.begin(), lowerKey.end(), lowerKey.begin(), ::tolower);
        if (lowerKey == HEADER_CONNECTION || lowerKey == HEADER_AUTHORIZATION) {
            continue;
        }
        if (first) {
            first = false;
            ss << lowerKey;
        } else {
            ss << ";" << lowerKey;
        }
    }
    return ss.str();
}

std::string HttpRequest::GetCanonicalUri(bool isEscape)
{
    static re2::RE2 regex(R"(https?://[^/]+(/[^?]*))");
    std::string res;
    if (RE2::PartialMatch(url_, regex, &res)) {
        return isEscape ? EscapeURL(res, true) : res;
    }
    return "/";
}

Status HttpRequest::GetCanonicalRequest(bool isEscape, std::string &canonicalRequest)
{
    std::string hashVal;
    RETURN_IF_NOT_OK(GetBodyHash(hashVal));
    std::stringstream ss;
    ss << HttpMethodStr(method_) << '\n'
       << GetCanonicalUri(isEscape) << '\n' + GetCanonicalQueryParams(isEscape) << '\n'
       << GetCanonicalHeaders() << '\n'
       << GetSignedHeaders() << '\n'
       << hashVal;
    canonicalRequest = ss.str();
    return Status::OK();
}

void HttpRequest::ConcatenateQueryParams()
{
    if (queryParams_.empty()) {
        return;
    }
    std::stringstream ss;
    ss << url_ << "/?";
    for (auto it = queryParams_.begin(); it != queryParams_.end(); ++it) {
        ss << it->first;
        if (!it->second.empty()) {
            ss << "=" << it->second;
        }
        if (std::next(it) != queryParams_.end()) {
            ss << "&";
        }
    }
    url_ = ss.str();
}
}  // namespace datasystem