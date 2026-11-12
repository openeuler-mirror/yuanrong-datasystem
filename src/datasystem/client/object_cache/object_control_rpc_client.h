/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_OBJECT_CONTROL_RPC_CLIENT_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_OBJECT_CONTROL_RPC_CLIENT_H

#include <memory>
#include <string>
#include <vector>

#include "datasystem/client/object_cache/transport/rpc/exist_types.h"
#include "datasystem/client/object_cache/transport/rpc/set_request_builder.h"
#include "datasystem/client/object_cache/transport/rpc/worker_rpc_dispatcher.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/protos/object_posix.pb.h"

namespace datasystem {
namespace client {

class ObjectControlRpcClient {
public:
    explicit ObjectControlRpcClient(std::shared_ptr<WorkerRpcDispatcher> dispatcher = nullptr);
    virtual ~ObjectControlRpcClient() = default;

    virtual Status Exist(const HostPort &workerAddr, const TransportExistRequest &input,
                         TransportExistResult &output);

    virtual Status Delete(const HostPort &workerAddr, const TransportRequestContext &context,
                          const std::vector<std::string> &objectKeys, bool areDeviceObjects, bool isRouted,
                          DeleteAllCopyRspPb &response, RpcExecutionState *executionState = nullptr);

    virtual Status GetObjMetaInfo(const HostPort &workerAddr, const std::string &tenantId,
                                  const std::vector<std::string> &objectKeys, std::vector<ObjMetaInfo> &objMetas);

    virtual Status QuerySize(const HostPort &workerAddr, const TransportRequestContext &context,
                             const std::vector<std::string> &objectKeys, QuerySizeRspPb &response);

    virtual Status Expire(const HostPort &workerAddr, const TransportRequestContext &context,
                          const std::vector<std::string> &objectKeys, uint32_t ttlSeconds, ExpireRspPb &response,
                          RpcExecutionState *executionState = nullptr);

    virtual Status GetMetaInfo(const HostPort &workerAddr, const TransportRequestContext &context,
                               const std::vector<std::string> &objectKeys, bool isDeviceKey, bool isRouted,
                               GetMetaInfoRspPb &response);

private:
    std::shared_ptr<WorkerRpcDispatcher> dispatcher_;
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_OBJECT_CONTROL_RPC_CLIENT_H
