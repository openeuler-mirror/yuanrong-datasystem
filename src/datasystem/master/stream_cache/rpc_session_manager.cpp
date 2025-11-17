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
#include "datasystem/master/stream_cache/rpc_session_manager.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace master {

Status RpcSessionManager::GetRpcSession(const HostPort &endPoint, std::shared_ptr<MasterWorkerSCApi> &rpcStub,
                                        const std::shared_ptr<AkSkManager> &akSkManager)
{
    rpcStub = MasterWorkerSCApi::CreateMasterWorkerSCApi(endPoint, localMaster_, akSkManager, masterWorkerSvc_.get());
    return rpcStub->Init();
}
}  // namespace master
}  // namespace datasystem
