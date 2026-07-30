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

#include "datasystem/coordinator_server.h"

#include <condition_variable>
#include <csignal>
#include <memory>

#include "datasystem/common/signal/signal.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/coordinator/coordinator_runtime.h"

namespace datasystem {

std::condition_variable g_termSignalCv;

namespace {

void SignalHandler(int signum)
{
    (void)signum;
    g_exitFlag = 1;
}

}  // namespace

CoordinatorServer::CoordinatorServer() : runtime_(std::make_unique<CoordinatorRuntime>())
{
    (void)signal(SIGPIPE, SIG_IGN);
    (void)signal(SIGINT, SignalHandler);
    (void)signal(SIGTERM, SignalHandler);
}

CoordinatorServer::~CoordinatorServer() = default;

CoordinatorServer *CoordinatorServer::GetInstance()
{
    static CoordinatorServer instance;
    return &instance;
}

Status CoordinatorServer::InitAndRun()
{
    return runtime_->InitAndRun();
}

Status CoordinatorServer::InitAndRun(const CoordinatorOptions &options)
{
    CHECK_FAIL_RETURN_STATUS(!options.configFilePath.empty(), K_INVALID,
                             "configFilePath must not be empty");
    return runtime_->InitAndRun(options);
}

Status CoordinatorServer::Stop()
{
    return runtime_->Stop();
}

}  // namespace datasystem
