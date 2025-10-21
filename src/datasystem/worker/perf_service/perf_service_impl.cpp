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

#include "datasystem/worker/perf_service/perf_service_impl.h"

#include <string>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/perf_posix.pb.h"

namespace datasystem {
Status PerfServiceImpl::GetPerfLog(const datasystem::GetPerfLogReqPb &req, datasystem::GetPerfLogRspPb &rsp)
{
    if (akSkManager_->SystemAuthEnabled()) {
        CHECK_FAIL_RETURN_STATUS(!req.signature().empty(), K_NOT_AUTHORIZED, "AK/SK not provide");
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed");
    }
    LOG(INFO) << "Start to query perf log from PerfManager";
    std::vector<std::pair<std::string, PerfInfo>> perfInfoList;
    PerfManager::Instance()->GetPerfInfoList(perfInfoList);
    for (auto &info : perfInfoList) {
        GetPerfLogRspPb::PerfLogPb perfLog;
        uint64_t count = info.second.count;
        if (count == 0) {
            continue;
        }
        perfLog.set_node_type("worker");
        perfLog.set_key_name(info.first);
        perfLog.set_count(count);
        perfLog.set_min_time(info.second.minTime);
        perfLog.set_max_time(info.second.maxTime);
        perfLog.set_total_time(info.second.totalTime);
        perfLog.set_avg_time(info.second.totalTime / count);
        perfLog.set_max_frequency(info.second.maxFrequency);
        rsp.mutable_perf_logs()->Add(std::move(perfLog));
    }
    return Status::OK();
}

Status PerfServiceImpl::ResetPerfLog(const datasystem::ResetPerfLogReqPb &req, datasystem::ResetPerfLogRspPb &rsp)
{
    (void)rsp;
    if (akSkManager_->SystemAuthEnabled()) {
        CHECK_FAIL_RETURN_STATUS(!req.signature().empty(), K_NOT_AUTHORIZED, "AK/SK not provide");
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed");
    }
    LOG(INFO) << "Start to reset perf log from PerfManager";
    PerfManager::Instance()->ResetPerfLog();
    return Status::OK();
}

}  // namespace datasystem