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
#ifndef DATASYSTEM_COMMON_HTTPCLIENT_HTTP_REQUEST_H
#define DATASYSTEM_COMMON_HTTPCLIENT_HTTP_REQUEST_H

#include <string>

#include "datasystem/common/httpclient/http_message.h"
#include "datasystem/utils/status.h"

namespace datasystem {
const std::string EMPTY_CONTENT_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const std::string HEADER_CONNECTION = "connection";
const std::string HEADER_AUTHORIZATION = "authorization";

/**
 * @brief Escape the url.
 * @param[in] url The url.
 * @param[in] replacePath Replace path or not.
 * @return std::string The escaped url.
 */
std::string EscapeURL(const std::string &url, bool replacePath);

class HttpRequest : public HttpMessage {
public:
    HttpRequest();

    virtual ~HttpRequest() = default;

    /**
     * @brief set the request whole url,including path
     * @param[in] url request url
     */
    void SetUrl(std::string &&url);

    /**
     * @brief get the request url
     * @return request url
     */
    const std::string &GetUrl();

    /**
     * @brief set the request method
     * @param[in] method  request method
     */
    void SetMethod(HttpMethod &&method);

    /**
     * @brief get the request method
     * @return request method
     */
    const HttpMethod &GetMethod();

    /**
     * @brief Get the http method string.
     * @param[in] method The http method.
     * @return std::string The http method string.
     */
    static std::string HttpMethodStr(HttpMethod method)
    {
        switch (method) {
            case HttpMethod::GET:
                return "GET";
            case HttpMethod::PUT:
                return "PUT";
            case HttpMethod::POST:
                return "POST";
            case HttpMethod::DELETE:
                return "DELETE";
            default:
                return "Unknown";
        }
    }

    /**
     * @brief set the request timeout million second
     * @param[in] requestTimeoutMs request time out million second
     */
    void SetRequestTimeoutMs(int64_t requestTimeoutMs);

    /**
     * @brief get the request timeout million second
     * @return request time out million second
     */
    uint64_t GetRequestTimeoutMs();

    /**
     * @brief set the connect timeout million second
     * @param[in] connectTimeoutMs connect time out million second
     */
    void SetConnectTimeoutMs(int64_t connectTimeoutMs);

    /**
     * @brief get the connect timeout million second
     * @return connect time out million second
     */
    uint64_t GetConnectTimeoutMs();

    /**
     * @brief the httpclient will call BuildRequest, build the request before send action
     */
    virtual void BuildRequest();

    /**
     * @brief the httpclient will call ClearSensitiveInfo after the request finish.
     * if request has sensitive info, user should implement this function.
     */
    virtual void ClearSensitiveInfo();

    /**
     * @brief Get the time of this request being in the async queue
     * @return The microseconds spend in async queue of cloud storage
     */
    uint64_t GetAsyncElapseTime();

    /**
     * @brief Set the time of this request being in the async queue
     */
    void SetAsyncElapseTime(int64_t asyncElapseTime);

    /**
     * @brief Get the Signed Headers.
     * @return std::string The Signed Headers.
     */
    std::string GetSignedHeaders();

    /**
     * @brief Get the Canonical Request.
     * @param[in] isEscape Escape the uri and query params or not.
     * @param[out] canonicalRequest The Canonical Request.
     * @return Status of the call.
     */
    Status GetCanonicalRequest(bool isEscape, std::string &canonicalRequest);

    /**
     * @brief Concatenate the query params.
     */
    void ConcatenateQueryParams();

private:
    /**
     * @brief Get the Body Hash.
     * @param[in] hashVal The hash of body.
     * @return Status of the call.
     */
    Status GetBodyHash(std::string &hashVal);

    /**
     * @brief Get the Canonical Queries.
     * @param[in] isEscape Escape the query params or not.
     * @return std::string The Canonical Queries.
     */
    std::string GetCanonicalQueryParams(bool isEscape);

    /**
     * @brief Get the Canonical Headers.
     * @return std::string The Canonical Headers.
     */
    std::string GetCanonicalHeaders();

    /**
     * @brief Get the Canonical Uri.
     * @param[in] isEscape Escape the uri or not.
     * @return std::string The Canonical Uri.
     */
    std::string GetCanonicalUri(bool isEscape);

    int64_t requestTimeoutMs_{10000L};
    int64_t connectTimeoutMs_{10000L};
    HttpMethod method_;
    std::string url_;
    uint64_t asyncElapse_;
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_HTTPCLIENT_HTTP_REQUEST_H
