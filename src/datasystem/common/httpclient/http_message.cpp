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
 * Description: interface of http message
 */
#include "datasystem/common/httpclient/http_message.h"

namespace datasystem {
void HttpMessage::AddHeader(const std::string &name, const std::string &value)
{
    headers_[name] = value;
}

void HttpMessage::AddHeader(std::string &&name, std::string &&value)
{
    headers_[std::move(name)] = std::move(value);
}

const std::string &HttpMessage::GetHeader(const std::string &name)
{
    return headers_[name];
}

void HttpMessage::RemoveHeader(const std::string &name)
{
    headers_.erase(name);
}

void HttpMessage::RemoveAllHeader()
{
    headers_.clear();
}

const std::map<std::string, std::string> &HttpMessage::Headers() const
{
    return headers_;
}

void HttpMessage::SetBody(const std::shared_ptr<std::iostream> &body)
{
    body_ = body;
}

std::shared_ptr<std::iostream> &HttpMessage::GetBody()
{
    return body_;
}

void HttpMessage::AddQueryParam(const std::string &name, const std::string &value)
{
    queryParams_[name] = value;
}

const std::map<std::string, std::string> &HttpMessage::GetQueryParams() const
{
    return queryParams_;
}

}  // namespace datasystem