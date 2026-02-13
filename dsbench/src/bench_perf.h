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

#ifndef BENCH_PERF_H
#define BENCH_PERF_H

#include <map>
#include <string>

#include "datasystem/perf_client.h"
#include "datasystem/utils/status.h"

#include "args_base.h"

namespace datasystem {
namespace bench {
#ifdef ENABLE_PERF
class PerfManager {
public:
    PerfManager(const ArgsBase &args);

    Status Init();

    Status SaveAllPerfLog();

    Status ResetPerfLog();

private:
    Status SavePerfLog(const std::string &command, const std::string &workerAddr);

    std::map<std::string, std::unique_ptr<PerfClient>> perfClients_;
    bool supportPerf_;
    std::string action_;
    std::string perfWorkersStr_;
    std::string perfPath_;
    std::string accessKey_;
    std::string secretKey_;
};
#else
class PerfManager {
public:
    PerfManager(const ArgsBase &)
    {
    }

    Status Init()
    {
        return Status::OK();
    }

    Status SaveAllPerfLog()
    {
        return Status::OK();
    }

    Status ResetPerfLog()
    {
        return Status::OK();
    }
};
#endif

}  // namespace bench
}  // namespace datasystem

#endif  // BENCH_PERF_H
