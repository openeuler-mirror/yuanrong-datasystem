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
 * Description: Basic Descriptor class that records GCS information.
 */
#include "datasystem/master/node_descriptor.h"

#include "datasystem/common/log/log.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/validator.h"


DS_DECLARE_uint32(node_timeout_s);
DS_DECLARE_uint32(node_dead_timeout_s);

namespace datasystem {
namespace master {
NodeDescriptor::NodeDescriptor(NodeTypePb type, const HostPort nodeAddress)
    : type_(type), nodeAddress_(std::move(nodeAddress)), timeOut_(false), state_(NodeState::RUNNING)
{
}

void NodeDescriptor::UpdateHeartbeatTime()
{
    lastHeartbeat_.Reset();
}

Status NodeDescriptor::UpdateHostPort(const HostPort &hostPort)
{
    nodeAddress_ = hostPort;
    return Status::OK();
}

bool NodeDescriptor::EvaluateTimeout() const
{
    return (lastHeartbeat_.ElapsedSecond() > FLAGS_node_timeout_s);
}

bool NodeDescriptor::AssumeDead() const
{
    return (lastHeartbeat_.ElapsedSecond() > FLAGS_node_dead_timeout_s);
}

Status NodeDescriptor::ProcessHeartbeatMsg(const HeartbeatReqPb &req, HeartbeatRspPb &rsp)
{
    (void)req;
    (void)rsp;
    UpdateHeartbeatTime();
    return Status::OK();
}
}  // namespace master
}  // namespace datasystem
