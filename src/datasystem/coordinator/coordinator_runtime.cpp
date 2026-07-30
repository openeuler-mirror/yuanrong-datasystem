// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "datasystem/coordinator/coordinator_runtime.h"

#include <chrono>
#include <exception>
#include <utility>

#include "datasystem/common/coordinator/static_coordinator_discovery.h"
#include "datasystem/common/flags/dynamic_flag_config.h"
#include "datasystem/common/flags/flag_manager.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/operation_logger.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/signal/signal.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/coordinator/coordinator_service_impl.h"

DS_DECLARE_string(coordinator_address);
DS_DECLARE_string(coordinator_raft_initial_peers);
DS_DECLARE_string(coordinator_raft_data_dir);
DS_DECLARE_int32(coordinator_raft_heartbeat_interval_ms);
DS_DECLARE_int32(coordinator_raft_election_timeout_ms);
DS_DECLARE_uint32(coordinator_member_failure_grace_ms);
DS_DECLARE_uint32(coordinator_discovery_retry_interval_ms);

namespace datasystem {
namespace {

constexpr int kStopPollIntervalMs = 100;

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

void PreserveFirstError(Status &firstError, const Status &status, const char *operation)
{
    if (status.IsOk()) {
        return;
    }
    if (firstError.IsOk()) {
        firstError = status;
        return;
    }
    LOG(ERROR) << operation << " failed after an earlier Coordinator lifecycle error, status=" << status.ToString();
}
}  // namespace

CoordinatorRuntime::CoordinatorRuntime() = default;

CoordinatorRuntime::~CoordinatorRuntime() noexcept
{
    (void)Stop();
    LOG_IF_ERROR(InvokeOnStop(), "Coordinator lifecycle onStop failed during Runtime destruction");
    LOG_IF_ERROR(ShutdownService(), "Coordinator shutdown failed during Runtime destruction");
}

Status CoordinatorRuntime::InitAndRun()
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    return InitAndRunInternal(nullptr);
}

Status CoordinatorRuntime::InitAndRun(const CoordinatorOptions &options)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    RETURN_IF_NOT_OK(ValidateOptions(options));
    return InitAndRunInternal(&options);
}

Status CoordinatorRuntime::InitAndRunInternal(const CoordinatorOptions *options)
{
    DynamicFlagConfig flags;
    OperationLogger::Instance().LogConfigInit(flags.GetAllFlagsStr());

    Status firstError;
    std::shared_ptr<ICoordinatorDiscovery> coordinatorDiscovery;
    int expectedMemberCount = 0;
    do {
        if (options != nullptr) {
            onStart_ = options->onStart;
            onStop_ = options->onStop;
            coordinatorDiscovery = options->coordinatorDiscovery;
            expectedMemberCount = options->expectedMemberCount;
            if (!options->configFilePath.empty()) {
                std::string errMsg;
                CHECK_FAIL_RETURN_STATUS(
                    FlagManager::GetInstance()->ParseConfigFile(options->configFilePath, errMsg), K_INVALID,
                    FormatString("Parse config file %s error: %s", options->configFilePath, errMsg));
            }
        } else {
            onStart_ = [] { return Status::OK(); };
            onStop_ = [] { return Status::OK(); };
            const auto &peers = FLAGS_coordinator_raft_initial_peers.empty() ? FLAGS_coordinator_address
                                                                             : FLAGS_coordinator_raft_initial_peers;
            auto staticCoordinatorDiscovery = std::make_shared<StaticCoordinatorDiscovery>(peers);
            expectedMemberCount = static_cast<int>(staticCoordinatorDiscovery->GetCount());
            coordinatorDiscovery = std::move(staticCoordinatorDiscovery);
        }
        callbackState_ = LifecycleCallbackState::READY;

        auto raftFlags = GetRaftFlags();
        HostPort localAddress;
        RETURN_IF_NOT_OK(localAddress.ParseString(raftFlags.localAddress));

        service_ = std::make_unique<coordinator::CoordinatorServiceImpl>(localAddress, std::move(coordinatorDiscovery),
                                                                         expectedMemberCount, std::move(raftFlags));

        firstError = service_->Init();
        if (firstError.IsError()) {
            break;
        }
        firstError = service_->Start();
        if (firstError.IsError()) {
            break;
        }

        firstError = InvokeOnStart();
        if (firstError.IsError()) {
            break;
        }

        firstError = service_->StartElectionManager();
        if (firstError.IsError()) {
            break;
        }

        LOG(INFO) << "Coordinator started successfully, entering Runtime event loop";
        RunEventLoop();
    } while (false);

    PreserveFirstError(firstError, InvokeOnStop(), "Coordinator lifecycle onStop");
    PreserveFirstError(firstError, ShutdownService(), "Coordinator service shutdown");
    return firstError;
}

Status CoordinatorRuntime::Stop()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopRequested_ = true;
    }
    stopCv_.notify_all();
    return Status::OK();
}

bool CoordinatorRuntime::IsLeader() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return service_ != nullptr && service_->IsLeader();
}

Status CoordinatorRuntime::GetLeader(std::string &leaderAddress) const
{
    leaderAddress.clear();
    std::lock_guard<std::mutex> lock(mutex_);
    if (service_ == nullptr) {
        return Status(K_NOT_READY, "Coordinator Runtime service is not running");
    }
    return service_->GetLeader(leaderAddress);
}

coordinator::CoordinatorRaftFlags CoordinatorRuntime::GetRaftFlags() const
{
    return coordinator::CoordinatorRaftFlags{ FLAGS_coordinator_address,
                                              FLAGS_coordinator_raft_data_dir,
                                              FLAGS_coordinator_raft_heartbeat_interval_ms,
                                              FLAGS_coordinator_raft_election_timeout_ms,
                                              FLAGS_coordinator_discovery_retry_interval_ms,
                                              FLAGS_coordinator_member_failure_grace_ms };
}

Status CoordinatorRuntime::InvokeOnStart()
{
    std::function<Status()> callback;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (callbackState_ != LifecycleCallbackState::READY) {
            return Status::OK();
        }
        callback = std::move(onStart_);
        callbackState_ = LifecycleCallbackState::START_ATTEMPTED;
    }
    return InvokeLifecycleCallback(callback, "Coordinator lifecycle onStart");
}

Status CoordinatorRuntime::InvokeOnStop()
{
    std::function<Status()> callback;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (callbackState_ != LifecycleCallbackState::START_ATTEMPTED) {
            return Status::OK();
        }
        callback = std::move(onStop_);
        callbackState_ = LifecycleCallbackState::STOP_INVOKED;
    }
    return InvokeLifecycleCallback(callback, "Coordinator lifecycle onStop");
}

Status CoordinatorRuntime::ShutdownService()
{
    // Detach ownership first so a throwing Shutdown cannot leave a Service owned by the Runtime.
    std::unique_ptr<coordinator::CoordinatorServiceImpl> service;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        service = std::move(service_);
    }
    if (service == nullptr) {
        return Status::OK();
    }
    return service->Shutdown();
}

void CoordinatorRuntime::RunEventLoop()
{
    std::unique_lock<std::mutex> lock(mutex_);
    while (!stopRequested_ && !IsTermSignalReceived()) {
        stopCv_.wait_for(lock, std::chrono::milliseconds(kStopPollIntervalMs),
                         [this] { return stopRequested_ || IsTermSignalReceived(); });
    }
    lock.unlock();
    LOG(INFO) << "Coordinator Runtime stop requested, shutting down";
}

}  // namespace datasystem
