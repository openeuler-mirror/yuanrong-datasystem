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
 * Description: The interface of rpc session manager.
 */
#ifndef DATASYSTEM_MASTER_STREAM_CACHE_RPC_SESSION_MANAGER_H
#define DATASYSTEM_MASTER_STREAM_CACHE_RPC_SESSION_MANAGER_H

#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include "datasystem/master/stream_cache/master_worker_sc_api.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
class MasterWorkerSCServiceImpl;
}  // namespace stream_cache
}  // namespace worker
namespace master {
class RpcSessionManager {
public:
    /**
     * @brief Get RpcSession by endPoint.
     * @param[in] endPoint Worker endpoint address.
     * @param[out] rpcStub Pointer to target rpcSession.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     * @return Status of the call.
     */
    Status GetRpcSession(const HostPort &endPoint, std::shared_ptr<MasterWorkerSCApi> &rpcStub,
                         const std::shared_ptr<AkSkManager> &akSkManager);

    void SetLocalArgs(const HostPort &localMaster,
                      std::shared_ptr<worker::stream_cache::MasterWorkerSCServiceImpl> service)
    {
        localMaster_ = localMaster;
        masterWorkerSvc_ = service;
    }

private:
    HostPort localMaster_;
    std::shared_ptr<worker::stream_cache::MasterWorkerSCServiceImpl> masterWorkerSvc_{ nullptr };
};
}  // namespace master
}  // namespace datasystem
#endif  // DATASYSTEM_MASTER_STREAM_CACHE_RPC_SESSION_MANAGER_H
