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

#include "args_base.h"

#include <iostream>
#include <sstream>

#include "datasystem/common/util/version.h"
#include "datasystem/utils/status.h"
#include "kv/kv_args.h"

namespace datasystem {
namespace bench {
ArgsBase::ArgsBase(const std::string &command)
    : command(command), action(""), workerAddress(""), threadNum(1), perfPath("./perf.log")
{
}

Status ArgsBase::Create(int argc, char *argv[], std::unique_ptr<ArgsBase> &args, bool &shouldExit)
{
    if (argc <= 1) {
        PrintUsage(argv[0]);
        return Status(K_INVALID, "");
    }

    std::string command = argv[1];
    if (command == "kv") {
        args = std::make_unique<KVArgs>(command);
        return args->Parse(argc, argv);
    } else if (command == "-h") {
        shouldExit = true;
        PrintUsage(argv[0]);
        return Status::OK();
    } else if (command == "-v") {
        shouldExit = true;
        std::cout << "Version Information:\n";
        std::cout << "  Version:      " << DATASYSTEM_VERSION << "\n";
        std::cout << "  Git Commit:   " << GetGitHash() << "\n";
        return Status::OK();
    } else {
        std::cout << "error: unknown command " << command << std::endl;
        PrintUsage(argv[0]);
        return Status(K_INVALID, "");
    }

    return Status::OK();
}

void ArgsBase::PrintUsage(const std::string &argv0)
{
    std::stringstream ss;
    ss << "Usage:" << argv0 << " <command> [options]\n";
    ss << "Command:\n";
    ss << "  kv     Run benchmark for KVClient.\n";
    ss << "Options:\n";
    ss << "  -v     Show version\n";
    ss << "  -h     Show help\n";
    std::cout << ss.str();
}
}  // namespace bench
}  // namespace datasystem
