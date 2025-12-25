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

#include <csignal>
#include <iostream>

#include "args_base.h"
#include "bench_base.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"

using namespace datasystem::bench;
using namespace datasystem;

Status BenchMain(int argc, char **argv)
{
    (void)signal(SIGPIPE, SIG_IGN);
    std::unique_ptr<ArgsBase> args;
    std::unique_ptr<BenchBase> bench;
    bool shouldExit = false;
    RETURN_IF_NOT_OK(ArgsBase::Create(argc, argv, args, shouldExit));
    if (shouldExit) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(BenchBase::Create(args, bench));
    return bench->Start();
}

int main(int argc, char **argv)
{
    auto rc = BenchMain(argc, argv);
    if (rc.IsError()) {
        if (!rc.GetMsg().empty()) {
            std::cerr << "ERROR: dsbench_cpp execution failed. Reason: " << rc.GetMsg() << std::endl;
        }
        return 1;
    }
    return 0;
}
