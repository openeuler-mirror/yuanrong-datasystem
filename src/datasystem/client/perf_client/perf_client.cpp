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
#include "datasystem/perf_client.h"

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/client/perf_client/perf_client_worker_api.h"

namespace datasystem {
const static std::string CLIENT = "client";
const static std::string WORKER = "worker";

PerfClient::PerfClient(const ConnectOptions &connectOptions)
{
    Logging::GetInstance()->Start("perf_client", true);
    clientWorkerApi_ = std::make_shared<PerfClientWorkerApi>(std::move(connectOptions));
}

Status PerfClient::Init()
{
    return clientWorkerApi_->Init();
}

Status PerfClient::GetPerfLog(const std::string &type,
                              std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>> &perfLog)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    RETURN_IF_NOT_OK(CheckTypeParam(type));
    LOG(INFO) << "Start to get perf log, type is " << type;
    perfLog.clear();
    if (type == WORKER) {
        RETURN_IF_NOT_OK(clientWorkerApi_->GetPerfLog(perfLog));
    } else {
        std::vector<std::pair<std::string, PerfInfo>> perfInfoList;
        PerfManager::Instance()->GetPerfInfoList(perfInfoList);
        for (const auto &perfInfo : perfInfoList) {
            std::unordered_map<std::string, uint64_t> perfMap;
            perfMap["count"] = perfInfo.second.count;
            perfMap["min_time"] = perfInfo.second.minTime;
            perfMap["max_time"] = perfInfo.second.maxTime;
            perfMap["total_time"] = perfInfo.second.totalTime;
            perfMap["avg_time"] = perfInfo.second.totalTime / perfInfo.second.count;
            perfMap["max_frequency"] = perfInfo.second.maxFrequency;
            perfLog.emplace(perfInfo.first, std::move(perfMap));
        }
    }
    return Status::OK();
}

Status PerfClient::ResetPerfLog(const std::string &type)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    RETURN_IF_NOT_OK(CheckTypeParam(type));
    LOG(INFO) << "Start to reset perf log, type is " <<  type;
    if (type == WORKER) {
        RETURN_IF_NOT_OK(clientWorkerApi_->ResetPerfLog());
    } else {
        PerfManager::Instance()->ResetPerfLog();
    }
    return Status::OK();
}

Status PerfClient::CheckTypeParam(const std::string &type)
{
    if (type != CLIENT && type != WORKER) {
        return Status(K_INVALID, FormatString("The parameter type only support client or worker."));
    }
    return Status::OK();
}
}  // namespace datasystem