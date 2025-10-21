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
 * Description: GenericServiceImpl is used to implement common service of rpc server(worker/master/gcs).
 */
#ifndef DATASYSTEM_SERVER_GENERIC_SERIVICE_IMPL_H
#define DATASYSTEM_SERVER_GENERIC_SERIVICE_IMPL_H

#include "datasystem/protos/generic_service.service.rpc.pb.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
class GenericServiceImpl : public GenericService {
public:
    /**
     * @brief Create a new GenericServiceImpl object, it is used to define common service of rpc server.
     */
    explicit GenericServiceImpl(HostPort localAddress) : GenericService(localAddress){};

    ~GenericServiceImpl() = default;

    /**
     * @brief Flush coverage of rpc server(worker and master and gcs).
     * @return Status of the call.
     */
    Status GcovFlush(const GcovFlushReqPb &req, GcovFlushRspPb &resp) override;

    /**
     * @brief Set the inject point action.
     * @return Status of the call.
     */
    Status SetInjectAction(const datasystem::SetInjectActionReqPb &req, datasystem::SetInjectActionRspPb &rsp) override;

    /**
     * @brief Clear the inject point action.
     * @return Status of the call.
     */
    Status ClearInjectAction(const datasystem::ClearInjectActionReqPb &, datasystem::ClearInjectActionRspPb &) override;

    /**
     * @brief Get the inject point execute count.
     * @return Status of the call.
     */
    Status GetInjectActionExecuteCount(const datasystem::GetInjectActionExecuteCountReqPb &,
                                       datasystem::GetInjectActionExecuteCountRspPb &) override;
};
}  // namespace datasystem
#endif
