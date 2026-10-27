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
 * Description: PerfServiceImpl is used to collect performance logs.
 */
#ifndef DATASYSTEM_WORKER_PERF_SERVICE_IMPL_H
#define DATASYSTEM_WORKER_PERF_SERVICE_IMPL_H

#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/perf_posix.irpc.pb.h"
#include "datasystem/protos/perf_posix.service.rpc.pb.h"

namespace datasystem {
class PerfServiceImpl : public PerfService, public IPerfService {
public:
    /**
     * @brief Create a new GenericServiceImpl object, it is used to define common service of rpc server.
     */
    explicit PerfServiceImpl(HostPort address)
        : PerfService(address)
    {
    }

    ~PerfServiceImpl() = default;

    Status GetPerfLog(const datasystem::GetPerfLogReqPb &req, datasystem::GetPerfLogRspPb &rsp) override;
    Status ResetPerfLog(const datasystem::ResetPerfLogReqPb &req, datasystem::ResetPerfLogRspPb &rsp) override;
};
}  // namespace datasystem
#endif