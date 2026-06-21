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

/**
 * Description: Coordinator RPC service implementation skeleton.
 */
#ifndef DATASYSTEM_COORDINATOR_COORDINATOR_SERVICE_IMPL_H
#define DATASYSTEM_COORDINATOR_COORDINATOR_SERVICE_IMPL_H

#include <utility>

#include "datasystem/protos/coordinator.service.rpc.pb.h"

namespace datasystem {
namespace coordinator {
class CoordinatorServiceImpl : public CoordinatorService {
public:
    explicit CoordinatorServiceImpl(HostPort localAddress) : CoordinatorService(std::move(localAddress))
    {
    }
    ~CoordinatorServiceImpl() override = default;

    Status Put(const PutReqPb &req, PutRspPb &rsp) override;
    Status Range(const RangeReqPb &req, RangeRspPb &rsp) override;
    Status DeleteRange(const DeleteRangeReqPb &req, DeleteRangeRspPb &rsp) override;
    Status WatchRange(const WatchRangeReqPb &req, WatchRangeRspPb &rsp) override;
    Status KeepAlive(const KeepAliveReqPb &req, KeepAliveRspPb &rsp) override;
};
}  // namespace coordinator
}  // namespace datasystem
#endif  // DATASYSTEM_COORDINATOR_COORDINATOR_SERVICE_IMPL_H
