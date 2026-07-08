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

#include <chrono>
#include <condition_variable>
#include <csignal>
#include <mutex>

#include "datasystem/common/flags/flag_manager.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/signal/signal.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/coordinator/coordinator_service_impl.h"

DS_DECLARE_string(coordinator_address);

namespace datasystem {

std::condition_variable g_termSignalCv;

namespace {

constexpr int CHECK_EVERY_MS = 1000;

void SignalHandler(int signum)
{
    (void)signum;
    g_exitFlag = 1;
    g_termSignalCv.notify_all();
}

}  // namespace

CoordinatorServer::~CoordinatorServer()
{
    if (service_) {
        service_->Shutdown();
        service_.reset();
    }
}

CoordinatorServer *CoordinatorServer::GetInstance()
{
    static CoordinatorServer instance;
    return &instance;
}

Status CoordinatorServer::InitAndRun()
{
    {
        std::lock_guard<std::mutex> lock(initMutex_);
        if (isStarted_.load()) {
            return Status(K_INVALID, "CoordinatorServer already started");
        }

        RETURN_IF_NOT_OK(Init());
        auto rc = Start();
        if (rc.IsError()) {
            Shutdown();
            return rc;
        }

        isStarted_.store(true);
    }

    LOG(INFO) << "Coordinator started successfully";

    RunEventLoop();  // blocks until termination signal or Stop()

    return Shutdown();
}

Status CoordinatorServer::InitAndRun(const CoordinatorOptions &options)
{
    std::string errMsg;
    CHECK_FAIL_RETURN_STATUS(
        FlagManager::GetInstance()->ParseConfigFile(options.configFilePath, errMsg),
        K_INVALID, FormatString("Parse config file %s error: %s", options.configFilePath, errMsg));
    return InitAndRun();
}

Status CoordinatorServer::Stop()
{
    LOG(INFO) << "CoordinatorServer::Stop() called, requesting shutdown";
    g_exitFlag = 1;
    g_termSignalCv.notify_all();
    return Status::OK();
}

Status CoordinatorServer::Init()
{
    (void)signal(SIGPIPE, SIG_IGN);
    (void)signal(SIGINT, SignalHandler);
    (void)signal(SIGTERM, SignalHandler);

    HostPort coordinatorAddr;
    RETURN_IF_NOT_OK(coordinatorAddr.ParseString(FLAGS_coordinator_address));

    service_ = std::make_unique<coordinator::CoordinatorServiceImpl>(coordinatorAddr);
    return service_->Init();
}

Status CoordinatorServer::Start()
{
    return service_->Start();
}

Status CoordinatorServer::Shutdown()
{
    LOG(INFO) << "Coordinator process executing a shutdown.";
    if (service_) {
        service_->Shutdown();
        service_.reset();
    }
    LOG(INFO) << "Coordinator shutdown success.";
    return Status::OK();
}

void CoordinatorServer::RunEventLoop()
{
    std::unique_lock<std::mutex> termSignalLock(g_termSignalMutex);
    while (!IsTermSignalReceived()) {
        g_termSignalCv.wait_for(termSignalLock, std::chrono::milliseconds(CHECK_EVERY_MS),
                                [] { return IsTermSignalReceived(); });
    }

    LOG(INFO) << "Coordinator received termination signal, shutting down";
}

}  // namespace datasystem
