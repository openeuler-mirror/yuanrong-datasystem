/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Utility function for stream cache.
 */
#ifndef DATASYSTEM_COMMON_STREAM_CACHE_UTIL_H
#define DATASYSTEM_COMMON_STREAM_CACHE_UTIL_H

#include <memory>

#include "datasystem/protos/master_stream.service.rpc.pb.h"
#include "datasystem/protos/stream_posix.service.rpc.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
/**
    * @brief A helper function to flow error or success back on the unary rpc api.
    * @tparam reqT The request type for the unary rpc
    * @tparam rspT The response type for the unary rpc
    * @param[in] rc The status code to return back through the api
    * @param[in] rsp The response structure to return (if not an error case)
    * @param[in] errMsg The error message to log if the rc was an error case.
    * @param[in] serverApi The unary api to send responses on
    */
template <typename reqT, typename rspT>
inline void CheckErrorReturn(const Status &rc, const rspT &rsp, const std::string &errMsg,
                                std::shared_ptr<ServerUnaryWriterReader<rspT, reqT>> serverApi)
{
    if (rc.IsOk()) {
        // Success case, flow the response back to client (rc of OK is inferred)
        LOG_IF_ERROR(serverApi->Write(rsp), "Write reply to client stream failed");
    } else {
        // Error case, flow the rc back to client
        LOG(ERROR) << errMsg << rc.ToString();
        LOG_IF_ERROR(serverApi->SendStatus(rc), "Write reply to client stream failed");
    }
}
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_STREAM_CACHE_UTIL_H
