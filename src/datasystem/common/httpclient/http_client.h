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
 * Description: interface of httpclient
 */
#ifndef DATASYSTEM_COMMON_HTTPCLIENT_HTTP_CLIENT_H
#define DATASYSTEM_COMMON_HTTPCLIENT_HTTP_CLIENT_H

#include <memory>

#include "datasystem/common/httpclient/http_response.h"
#include "datasystem/common/httpclient/http_request.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/utils/status.h"

namespace datasystem {
    
static const int64_t HTTP_DEFAULT_TIMEOUT_MS = 10000;

class HttpClient {
public:
    HttpClient() = default;

    virtual ~HttpClient() = default;

    /**
     * @brief send the http request, the status_ in the HttpResponse will indicate whether the request is perform
     * success. The implement of this interface, should call request's BuildRequest before send request, should
     * call request's ClearSensitiveInfo where send finish.
     *
     * @param[in] request http request
     * @param[out] response http response
     * @return Status of the call.
     */
    virtual Status Send(const std::shared_ptr<HttpRequest> &request, std::shared_ptr<HttpResponse> &response) = 0;
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_HTTPCLIENT_HTTP_CLIENT_H
