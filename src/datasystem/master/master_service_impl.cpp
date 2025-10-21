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
 * Description: Implement common remote services on the master.
 */
#include "datasystem/master/master_service_impl.h"

#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/common/util/uuid_generator.h"

namespace datasystem {
namespace master {
MasterServiceImpl::MasterServiceImpl(HostPort serverAddress, std::shared_ptr<AkSkManager> akSkManager)
    : MasterService(std::move(serverAddress)), akSkManager_(akSkManager)
{
}

MasterServiceImpl::~MasterServiceImpl()
{
    LOG(INFO) << "MasterServiceImpl exit";
}

Status MasterServiceImpl::Init()
{
    return Status::OK();
}

Status MasterServiceImpl::Heartbeat(const HeartbeatReqPb &request, HeartbeatRspPb &response)
{
    (void)request;
    (void)response;
    return Status::OK();
}

}  // namespace master
}  // namespace datasystem
