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
 * Description: Client to l2 cache.
 */

#ifndef DATASYSTEM_COMMON_L2CACHE_L2CACHE_CLIENT_H
#define DATASYSTEM_COMMON_L2CACHE_L2CACHE_CLIENT_H

#include <memory>
#include <string>

#include "datasystem/common/httpclient/http_client.h"
#include "datasystem/common/httpclient/http_request.h"
#include "datasystem/common/httpclient/http_response.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
class GetObjectInfoListResp;
const std::string L2CACHE_PERCENT_SIGN_ENCODE = "%EF%BF%A5";
class L2CacheClient {
public:
    /**
     * @brief init the L2CacheClient
     * @return Status of the call
     */
    virtual Status Init() = 0;

    /**
     * @brief Upload the object content to l2 cache objectPath
     * @param[in] objectPath the object path in l2 cache bucket
     * @param[in] timeoutMs the connect and request timeout in million second
     * @param[in] body the object content
     * @param[in] asyncElapse The time this object being in the async queue
     * @return Status of the call
     */
    virtual Status Upload(const std::string &objectPath, int64_t timeoutMs, const std::shared_ptr<std::iostream> &body,
                          uint64_t asyncElapse = 0) = 0;

    /**
     * @brief get the object list from
     * @param[in] objectPrefix the object path in l2 cache bucket
     * @param[in] timeoutMs the connect and request timeout in million second
     * @param[in] listIncompleteVersions whether to list those incomplete versions. Usually they are partially uploaded.
     * @param[out] listResp the object listResp
     * @return Status of the call
     */
    virtual Status List(const std::string &objectPrefix, int64_t timeoutMs, bool listIncompleteVersions,
                        std::shared_ptr<GetObjectInfoListResp> &listResp) = 0;

    /**
     * @brief get the object content from l2 cache
     * @param[in] objectPath the object path in l2 cache bucket
     * @param[in] timeoutMs the connect and request timeout in million second
     * @param[out] content the object content
     * @return Status of the call
     */
    virtual Status Download(const std::string &objectPath, int64_t timeoutMs,
                            std::shared_ptr<std::stringstream> &content) = 0;

    /**
     * @brief delete the l2 cache object.
     * @param[in] objects the whole path of the object, not support prefix
     * @param[in] asyncElapse The time this object being in the async queue
     * @return Status of the call
     */
    virtual Status Delete(const std::vector<std::string> &objects, uint64_t asyncElapse = 0) = 0;

    virtual ~L2CacheClient() = default;

    /**
     * @brief we need url encode the objectPath for the below reason:
     *  1) l2cache not support continuous slash
     *  2) a objectPath contain # will cause objectPath truncate in l2cache
     *  3) objectPath abc and abc/123 will cause overlap when list all version
     *
     * @param[in] objectPath object path
     * @param[out] encodePath url encode and replace % to ￥
     * @return real object path in l2cache
     */
    static Status UrlEncode(const std::string &objectPath, std::string &encodePath);

    /**
     * @brief Obtains the request success rate of l2cache.
     * @return Success rate of l2cache request.
     */
    virtual std::string GetRequestSuccessRate() = 0;

protected:
    /**
     * @brief Send request to obs
     * @param[in] httpClient The http client
     * @param[in] request The request to be sent
     * @param[in] timeoutMs the connect and request timeout in million second
     * @param[out] response The response from obs
     * @return Status of the call
     */
    Status SendObsRequest(const std::shared_ptr<HttpClient> httpClient, const std::shared_ptr<HttpRequest> &request,
                          int64_t timeoutMs, std::shared_ptr<HttpResponse> &response);
};
}
#endif