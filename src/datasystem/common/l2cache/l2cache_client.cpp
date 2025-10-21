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

#include "l2cache_client.h"

#include <curl/curl.h>
#include <sstream>

#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/raii.h"

namespace datasystem {
Status L2CacheClient::UrlEncode(const std::string &objectPath, std::string &encodePath)
{
    std::ostringstream uniqSlash;
    CURL *curl = curl_easy_init();
    if (curl == nullptr) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Failed to init curl, encode the object key failed.");
    }
    Raii raii([&curl] { curl_easy_cleanup(curl); });
    char *urlEncode = curl_easy_escape(curl, objectPath.c_str(), objectPath.size());
    if (urlEncode == nullptr) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Failed to curl_easy_escape, encode the object key failed.");
    }

    Raii raiiEncode([&urlEncode] { curl_free(urlEncode); });
    std::string path(urlEncode);
    for (size_t i = 0; i < path.size(); i++) {
        if (path.at(i) == '%') {
            uniqSlash << L2CACHE_PERCENT_SIGN_ENCODE;
        } else {
            uniqSlash << path.at(i);
        }
    }
    encodePath = uniqSlash.str();
    return Status::OK();
}

Status L2CacheClient::SendObsRequest(const std::shared_ptr<HttpClient> httpClient,
                                     const std::shared_ptr<HttpRequest> &request, int64_t timeoutMs,
                                     std::shared_ptr<HttpResponse> &response)
{
    CHECK_FAIL_RETURN_STATUS(timeoutMs > 0, K_RPC_DEADLINE_EXCEEDED,
                             "The request process timeout before send request to l2cache.");
    request->SetConnectTimeoutMs(timeoutMs);
    request->SetRequestTimeoutMs(timeoutMs);

    PerfPoint point(PerfKey::OBS_HTTP_SEND);
    RETURN_IF_NOT_OK_APPEND_MSG(httpClient->Send(request, response), " when send request to obs.");
    point.Record();
    return Status::OK();
}
}  // namespace datasystem