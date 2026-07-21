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
#include <exception>
#include <mutex>

#include "datasystem/common/flags/dynamic_flag_config.h"
#include "datasystem/common/coordinator/static_coordinator_discovery.h"
#include "datasystem/common/flags/flag_manager.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/operation_logger.h"
#include "datasystem/common/log/trace.h"
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

void PreserveFirstError(Status &firstError, const Status &status, const char *operation)
{
    if (status.IsOk()) {
        return;
    }
    if (firstError.IsOk()) {
        firstError = status;
        return;
    }
    LOG(ERROR) << operation << " failed during cleanup, status=" << status.ToString();
}

Status ValidateOptions(const CoordinatorOptions &options)
{
    CHECK_FAIL_RETURN_STATUS(options.coordinatorDiscovery != nullptr, K_INVALID,
                             "coordinatorDiscovery must not be null");
    CHECK_FAIL_RETURN_STATUS(options.expectedMemberCount > 0, K_INVALID,
                             "expectedMemberCount must be greater than zero");
    CHECK_FAIL_RETURN_STATUS(static_cast<bool>(options.onStart) == static_cast<bool>(options.onStop), K_INVALID,
                             "onStart and onStop must be configured together");
    return Status::OK();
}

Status InvokeLifecycleCallback(const std::function<Status()> &callback, const char *callbackName)
{
    try {
        return callback();
    } catch (const std::exception &error) {
        return Status(K_RUNTIME_ERROR, FormatString("%s threw an exception: %s", callbackName, error.what()));
    } catch (...) {
        return Status(K_RUNTIME_ERROR, FormatString("%s threw an unknown exception", callbackName));
    }
}

}  // namespace

CoordinatorServer::~CoordinatorServer()
{
    LOG_IF_ERROR(InvokeOnStop(), "Coordinator lifecycle onStop failed during destruction");
    LOG_IF_ERROR(Shutdown(), "Coordinator shutdown failed during destruction");
}

CoordinatorServer *CoordinatorServer::GetInstance()
{
    static CoordinatorServer instance;
    return &instance;
}

Status CoordinatorServer::InitAndRun()
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    return InitAndRunInternal(nullptr);
}

Status CoordinatorServer::InitAndRun(const CoordinatorOptions &options)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    RETURN_IF_NOT_OK(ValidateOptions(options));
    return InitAndRunInternal(&options);
}

Status CoordinatorServer::InitAndRunInternal(const CoordinatorOptions *options)
{
    {
        std::lock_guard<std::mutex> lock(initMutex_);
        if (isStarted_.load()) {
            return Status(K_INVALID, "CoordinatorServer already started");
        }

        if (options != nullptr) {
            std::string errMsg;
            CHECK_FAIL_RETURN_STATUS(FlagManager::GetInstance()->ParseConfigFile(options->configFilePath, errMsg),
                                     K_INVALID,
                                     FormatString("Parse config file %s error: %s", options->configFilePath, errMsg));
            coordinatorDiscovery_ = options->coordinatorDiscovery;
            expectedMemberCount_ = options->expectedMemberCount;
            onStart_ = options->onStart;
            onStop_ = options->onStop;
        } else {
            coordinatorDiscovery_ = std::make_shared<StaticCoordinatorDiscovery>(FLAGS_coordinator_address);
            expectedMemberCount_ = 0;
            onStart_ = {};
            onStop_ = {};
        }
        callbackState_ = onStart_ ? LifecycleCallbackState::READY : LifecycleCallbackState::NOT_CONFIGURED;

        auto rc = Init();
        if (rc.IsError()) {
            LOG_IF_ERROR(Shutdown(), "Coordinator shutdown failed after initialization failure");
            return rc;
        }
        rc = Start();
        if (rc.IsError()) {
            LOG_IF_ERROR(Shutdown(), "Coordinator shutdown failed after start failure");
            return rc;
        }
        isStarted_.store(true);
    }

    LOG(INFO) << "Coordinator started successfully";
    DynamicFlagConfig flags;
    OperationLogger::Instance().LogConfigInit(flags.GetAllFlagsStr());

    Status firstError = Status::OK();
    if (!IsTermSignalReceived()) {
        PreserveFirstError(firstError, InvokeOnStart(), "Coordinator lifecycle onStart");
    }
    if (firstError.IsOk() && !IsTermSignalReceived()) {
        RunEventLoop();
    }
    PreserveFirstError(firstError, InvokeOnStop(), "Coordinator lifecycle onStop");
    PreserveFirstError(firstError, Shutdown(), "Coordinator shutdown");
    return firstError;
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

Status CoordinatorServer::InvokeOnStart()
{
    if (callbackState_ != LifecycleCallbackState::READY) {
        return Status::OK();
    }
    callbackState_ = LifecycleCallbackState::START_ATTEMPTED;
    return InvokeLifecycleCallback(onStart_, "Coordinator lifecycle onStart");
}

Status CoordinatorServer::InvokeOnStop()
{
    if (callbackState_ != LifecycleCallbackState::START_ATTEMPTED) {
        return Status::OK();
    }
    callbackState_ = LifecycleCallbackState::STOP_INVOKED;
    return InvokeLifecycleCallback(onStop_, "Coordinator lifecycle onStop");
}

Status CoordinatorServer::Shutdown()
{
    LOG(INFO) << "Coordinator process executing a shutdown.";
    Status status = Status::OK();
    if (service_) {
        status = service_->Shutdown();
        service_.reset();
    }
    coordinatorDiscovery_.reset();
    onStart_ = {};
    onStop_ = {};
    expectedMemberCount_ = 0;
    callbackState_ = LifecycleCallbackState::NOT_CONFIGURED;
    LOG(INFO) << "Coordinator shutdown finished, status=" << status.ToString();
    return status;
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
