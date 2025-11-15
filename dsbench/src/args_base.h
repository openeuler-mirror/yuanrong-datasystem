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

#ifndef ARGS_BASE_H
#define ARGS_BASE_H
#include <memory>
#include <string>
#include "datasystem/utils/status.h"

namespace datasystem {
namespace bench {
struct ArgsBase {
    ArgsBase(const std::string &command);
    static Status Create(int argc, char *argv[], std::unique_ptr<ArgsBase> &args, bool &shouldExit);
    static void PrintUsage(const std::string &argv0);
    virtual Status Parse(int argc, char *argv[]) = 0;
    bool ShouldExit();

    std::string command;
    std::string action;
    std::string workerAddress;
    uint64_t threadNum;
    std::string perfPath;
    std::string perfWorkers;
    std::string accessKey;
    std::string secretKey;
};
}  // namespace bench
}  // namespace datasystem
#endif
