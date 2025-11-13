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

#include <sstream>

#include "datasystem/common/perf/perf_manager.h"

namespace datasystem {
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