/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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

#include "bench_perf.h"

#include <sstream>
#include <unistd.h>
#include <pwd.h>
#include <fcntl.h>
#include <iostream>
#include <set>

#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uri.h"

namespace datasystem {
namespace bench {

#ifdef ENABLE_PERF
PerfManager::PerfManager(const ArgsBase &args)
    : supportPerf_(true),
      action_(args.action),
      perfWorkersStr_(args.perfWorkers.empty() ? args.workerAddress : args.perfWorkers),
      perfPath_(args.perfPath),
      accessKey_(args.accessKey),
      secretKey_(args.secretKey)
{
}

Status PerfManager::Init()
{
    auto perfWorkerAddrs = Split(perfWorkersStr_, ",");
    std::set<std::string> uniquePerfWorkerAddrs = { perfWorkerAddrs.begin(), perfWorkerAddrs.end() };

    for (const auto &worker : uniquePerfWorkerAddrs) {
        HostPort hostPort;
        auto rc = hostPort.ParseString(worker);
        if (rc.IsError()) {
            std::cerr << "Worker address parse error: " << rc.GetMsg() << std::endl;
            supportPerf_ = false;
            return rc;
        }

        ConnectOptions connectOptions = { .host = hostPort.Host(), .port = hostPort.Port() };
        connectOptions.accessKey = accessKey_;
        connectOptions.secretKey = secretKey_;
        auto client = std::make_unique<PerfClient>(connectOptions);
        rc = client->Init();
        if (rc.IsError()) {
            std::cerr << "PerfClient init error: " << rc.GetMsg() << std::endl;
            supportPerf_ = false;
            return rc;
        }
        perfClients_.emplace(worker, std::move(client));
    }

    return Status::OK();
}

Status PerfManager::SavePerfLog(const std::string &type, const std::string &workerAddr)
{
    if (!supportPerf_ || perfPath_.empty()) {
        return Status::OK();
    }

    std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>> perfLog;
    auto it = perfClients_.find(workerAddr);
    if (it == perfClients_.end()) {
        return Status::OK();
    }
    auto &perfClient = it->second;
    RETURN_IF_NOT_OK(perfClient->GetPerfLog(type, perfLog));

    std::stringstream ss;
    for (auto &item : perfLog) {
        ss << action_ << ",";
        ss << type;
        if (type == "worker") {
            ss << " " << workerAddr;
        }
        ss << ",";
        ss << item.first << ",";
        ss << item.second["avg_time"] << ",";
        ss << item.second["count"] << ",";
        ss << item.second["min_time"] << ",";
        ss << item.second["max_time"] << ",";
        ss << item.second["total_time"] << ",";
        ss << item.second["max_frequency"];
        ss << "\n";
    }
    auto output = ss.str();

    std::string finalPerfPath = perfPath_;

    const std::string kDefaultPerfPath = "~/.datasystem/perf";
    const std::string kAdditionalPerfPath = "";

    Status status = Uri::NormalizePathWithUserHomeDir(finalPerfPath, kDefaultPerfPath, kAdditionalPerfPath);
    if (!status.OK()) {
        return status;
    }

    const int fileMode = 0640;
    int fd = open(finalPerfPath.c_str(), O_WRONLY | O_CREAT | O_APPEND, fileMode);
    if (fd < 0) {
        std::string errMsg = "Failed to open perf log file: " + std::string(strerror(errno));
        std::cerr << errMsg << std::endl;
        return Status(K_RUNTIME_ERROR, __LINE__, __FILE__, errMsg);
    }

    int sz = write(fd, output.c_str(), output.size());
    if (sz < 0) {
        std::string errMsg = "Failed to write to perf log file: " + std::string(strerror(errno));
        std::cerr << errMsg << std::endl;
        RETRY_ON_EINTR(close(fd));
        return Status(K_RUNTIME_ERROR, __LINE__, __FILE__, errMsg);
    }

    RETRY_ON_EINTR(close(fd));

    return Status::OK();
}

Status PerfManager::SaveAllPerfLog()
{
    if (!supportPerf_ || perfClients_.empty() || perfPath_.empty()) {
        std::cout << "exit ahead" << std::endl;
        return Status::OK();
    }

    bool isFirst = true;
    for (auto &item : perfClients_) {
        auto &workerAddr = item.first;
        if (isFirst) {
            RETURN_IF_NOT_OK(SavePerfLog("client", workerAddr));
            isFirst = false;
        }
        RETURN_IF_NOT_OK(SavePerfLog("worker", workerAddr));
    }

    return Status::OK();
}

Status PerfManager::ResetPerfLog()
{
    if (!supportPerf_ || perfClients_.empty()) {
        std::cout << "exit ahead" << std::endl;
        return Status::OK();
    }

    bool isFirst = true;
    for (auto &item : perfClients_) {
        auto &perfClient = item.second;
        if (isFirst) {
            RETURN_IF_NOT_OK(perfClient->ResetPerfLog("client"));
            isFirst = false;
        }
        RETURN_IF_NOT_OK(perfClient->ResetPerfLog("worker"));
    }

    return Status::OK();
}
#endif
}  // namespace bench
}  // namespace datasystem
