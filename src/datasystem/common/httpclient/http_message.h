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
 * Description: abstract of http req or response message
 */
#ifndef DATASYSTEM_COMMON_HTTPCLIENT_HTTP_MESSAGE_H
#define DATASYSTEM_COMMON_HTTPCLIENT_HTTP_MESSAGE_H

#include <iostream>
#include <map>
#include <memory>
#include <string>

namespace datasystem {
enum class HttpMethod {GET, PUT, POST, DELETE };

class HttpMessage {
public:
    HttpMessage() = default;

    virtual ~HttpMessage() = default;

    /**
     * @brief add header info
     * @param[in] name header name
     * @param[in] value header value
     */
    void AddHeader(const std::string &name, const std::string &value);

    /**
     * @brief add header info
     * @param[in] name header name
     * @param[in] value header value
     */
    void AddHeader(std::string &&name, std::string &&value);

    /**
     * @brief remove specific header
     * @param[in] name the header name
     */
    void RemoveHeader(const std::string &name);

    /**
     * @brief remove all header fields.
     */
    void RemoveAllHeader();

    /**
     * @brief get the specific header
     * @param[in] name header name
     * @return header value
     */
    const std::string &GetHeader(const std::string &name);

    /**
     * @brief get headers info
     * @return headers info
     */
    const std::map<std::string, std::string> &Headers() const;

    /**
     * @brief set the body stream.
     * @param[in] body body stream
     */
    void SetBody(const std::shared_ptr<std::iostream> &body);

    /**
     * @brief get the body stream.
     * @return the body stream
     */
    std::shared_ptr<std::iostream> &GetBody();

    /**
     * @brief add query param.
     * @param[in] name query key.
     * @param[in] value query value.
     */
    void AddQueryParam(const std::string &name, const std::string &value);

    /**
     * @brief get query params
     * @return query params
     */
    const std::map<std::string, std::string> &GetQueryParams() const;

protected:
    std::map<std::string, std::string> headers_;
    std::map<std::string, std::string> queryParams_;

private:
    std::shared_ptr<std::iostream> body_;
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_HTTPCLIENT_HTTP_MESSAGE_H
