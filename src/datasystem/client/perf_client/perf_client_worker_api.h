/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
* Description: Define api for accessing perf service.
*/
#ifndef DATASYSTEM_PERF_CLIENT_WORKER_API_H
#define DATASYSTEM_PERF_CLIENT_WORKER_API_H

#include <unordered_map>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/rpc/rpc_auth_keys.h"
#include "datasystem/common/rpc/rpc_credential.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/perf_posix.stub.rpc.pb.h"
#include "datasystem/utils/connection.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"

namespace datasystem {
class PerfClientWorkerApi {
public:
   explicit PerfClientWorkerApi(const ConnectOptions &connectOptions);

   /**
    * @brief Initialize AdminWorkerApi.
    * @param[in] cred The authentication credential.
    * @return Status of the call.
    */
   Status Init();

   /**
    * @brief Get the perf log list.
    * @param[out] perfLog The performance information. Each element of the map represent one perf log for
    * worker/master, and every unordered_map contains the key: "count", "min_time", "max_time", "total_time",
    * "avg_time", "avg_time", "max_frequency".
    * @return Status of the call.
    */
   Status GetPerfLog(std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>> &perfLog);

   /**
    * @brief Reset the perf log.
    * @return Status of the call.
    */
   Status ResetPerfLog();

private:
    HostPort hostPort_;
    std::unique_ptr<Signature> signature_{ nullptr };
    RpcAuthKeys authKeys_;

    std::unique_ptr<PerfService_Stub> rpcSession_{ nullptr };
};
}  // namespace datasystem

#endif  // DATASYSTEM_PERF_CLIENT_WORKER_API_H
