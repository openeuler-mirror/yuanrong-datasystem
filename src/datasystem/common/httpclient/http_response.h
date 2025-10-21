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
 * Description: http response
 */
#ifndef DATASYSTEM_COMMON_HTTPCLIENT_HTTP_RESPONSE_H
#define DATASYSTEM_COMMON_HTTPCLIENT_HTTP_RESPONSE_H

#include "datasystem/common/httpclient/http_message.h"

namespace datasystem {
class HttpResponse : public HttpMessage {
public:
    constexpr static int STATUS_CLIENT_ERR = -1;
    constexpr static int HTTP_STATUS_CODE_OK = 200;
    constexpr static int HTTP_STATUS_CODE_MULTIPLE_CHOICE = 300;
    constexpr static int HTTP_STATUS_CODE_NOT_FOUND = 404;

    HttpResponse();

    ~HttpResponse() override;

    /**
     * @brief set the response status
     * @param[in] status response status
     */
    void SetStatus(int status);

    /**
     * @brief get the response status
     * @return response status
     */
    int GetStatus() const;

    /**
     * @brief indicate whether the request is success.
     * @return true if success, otherwise false.
     */
    bool IsSuccess() const;

private:
    int status_ = STATUS_CLIENT_ERR;
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_HTTPCLIENT_HTTP_RESPONSE_H
