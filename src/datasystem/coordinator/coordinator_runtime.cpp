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

#include <cerrno>
#include <chrono>
#include <exception>
#include <utility>

#include <gflags/gflags.h>

#include "datasystem/common/coordinator/static_coordinator_discovery.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/flags/dynamic_config_updater.h"
#include "datasystem/common/flags/dynamic_flag_config.h"
#include "datasystem/common/flags/flag_manager.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/log/operation_logger.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/signal/signal.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/version.h"
#include "datasystem/coordinator/coordinator_service_impl.h"

DS_DECLARE_string(coordinator_address);
DS_DECLARE_string(coordinator_raft_initial_peers);
DS_DECLARE_string(coordinator_raft_data_dir);
DS_DECLARE_int32(coordinator_raft_heartbeat_interval_ms);
DS_DECLARE_int32(coordinator_raft_election_timeout_ms);
DS_DECLARE_uint32(coordinator_member_failure_grace_ms);
DS_DECLARE_uint32(coordinator_discovery_retry_interval_ms);
DS_DECLARE_int32(watch_event_dispatch_thread);
DECLARE_int32(task_group_ntags);
namespace datasystem {
namespace {

constexpr int kStopPollIntervalMs = 100;
constexpr int kWatchDispatcherBthreadTag = 1;
constexpr int kCoordinatorBthreadTagCount = 2;

bool IsCoordinatorRuntimeApplicableFlag(const std::string &flagName)
{
    return flagName == "request_sample_rate" || flagName == "access_sample_rate"
        || flagName == "diagnostic_sample_rate";
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

CoordinatorRuntime::CoordinatorRuntime() : runtimeFlags_(std::make_unique<DynamicFlagConfig>()),
                                           configUpdater_(std::make_unique<DynamicConfigUpdater>(
                                               *runtimeFlags_, IsCoordinatorRuntimeApplicableFlag))
{
}

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
    if (options != nullptr && !options->configFilePath.empty()) {
        std::string errMsg;
        CHECK_FAIL_RETURN_STATUS(
            FlagManager::GetInstance()->ParseConfigFile(options->configFilePath, errMsg), K_INVALID,
            FormatString("Parse config file %s error: %s", options->configFilePath, errMsg));
    }
    RETURN_IF_NOT_OK(InitWatchDispatcherBthreadPool());

    const std::string logFilename = FLAGS_log_filename.empty() ? "datasystem_coordinator" : FLAGS_log_filename;
    Logging::GetInstance()->Start(logFilename, LogProcessRole::COORDINATOR);
    Logging::GetInstance()->LogProcessVersion(GIT_HASH, GIT_BRANCH);
    Status firstError;
    std::shared_ptr<ICoordinatorDiscovery> coordinatorDiscovery;
    int expectedMemberCount = 0;
    auto bootstrapMode = coordinator::RaftBootstrapMode::DISCOVERY_OBSERVATION;
    do {
        if (options != nullptr) {
            onStart_ = options->onStart;
            onStop_ = options->onStop;
            coordinatorDiscovery = options->coordinatorDiscovery;
            expectedMemberCount = options->expectedMemberCount;
            LOG(INFO) << "Coordinator expect " << expectedMemberCount << " peers from CoordinatorDiscovery";
        } else {
            onStart_ = [] { return Status::OK(); };
            onStop_ = [] { return Status::OK(); };
            if (!FLAGS_coordinator_raft_initial_peers.empty()) {
                auto staticCoordinatorDiscovery =
                    std::make_shared<StaticCoordinatorDiscovery>(FLAGS_coordinator_raft_initial_peers);
                expectedMemberCount = static_cast<int>(staticCoordinatorDiscovery->GetCount());
                coordinatorDiscovery = std::move(staticCoordinatorDiscovery);
                bootstrapMode = coordinator::RaftBootstrapMode::STATIC_INITIAL_PEERS;
                LOG(INFO) << "Coordinator initialize with static peers:" << FLAGS_coordinator_raft_initial_peers;
            } else {
                LOG(INFO) << "Coordinator initialize in single-node no-election mode:" << FLAGS_coordinator_address;
            }
        }
        callbackState_ = LifecycleCallbackState::READY;

        OperationLogger::Instance().LogConfigInit(runtimeFlags_->GetAllFlagsStr());
        RETURN_IF_NOT_OK_APPEND_MSG(metrics::InitKvMetrics(), "\nCoordinator metrics initialization failed.");

        auto raftFlags = GetRaftFlags();
        HostPort localAddress;
        RETURN_IF_NOT_OK(localAddress.ParseString(raftFlags.localAddress));

        service_ = std::make_unique<coordinator::CoordinatorServiceImpl>(localAddress, std::move(coordinatorDiscovery),
                                                                         expectedMemberCount, std::move(raftFlags),
                                                                         watchDispatcherBthreadTag_, bootstrapMode);
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
        EnableConfigUpdates();

        LOG(INFO) << "Coordinator started successfully, entering Runtime event loop";
        RunEventLoop();
    } while (false);

    DisableConfigUpdates();
    PreserveFirstError(firstError, InvokeOnStop(), "Coordinator lifecycle onStop");
    PreserveFirstError(firstError, ShutdownService(), "Coordinator service shutdown");
    // Snapshot only after RPC ingress and watch dispatch are stopped, so the final delta includes drained work.
    metrics::PrintSummary();
    return firstError;
}

Status CoordinatorRuntime::InitWatchDispatcherBthreadPool()
{
    if (bthread_getconcurrency_by_tag(BTHREAD_TAG_DEFAULT) != EPERM) {
        return Status::OK();
    }
    FLAGS_task_group_ntags = kCoordinatorBthreadTagCount;
    CHECK_FAIL_RETURN_STATUS(
        bthread_setconcurrency_by_tag(FLAGS_watch_event_dispatch_thread, kWatchDispatcherBthreadTag) == 0, K_RUNTIME_ERROR,
        FormatString("Failed to configure watch event bthread pool, tag: %d, pthread count: %d",
                     kWatchDispatcherBthreadTag, FLAGS_watch_event_dispatch_thread));
    watchDispatcherBthreadTag_ = kWatchDispatcherBthreadTag;
    LOG(INFO) << "Coordinator watch notifications will use an isolated bthread pool, tag="
              << watchDispatcherBthreadTag_ << ", pthreadCount=" << FLAGS_watch_event_dispatch_thread;
    return Status::OK();
}

Status CoordinatorRuntime::UpdateConfig(const std::string &configJson)
{
    std::lock_guard<std::mutex> lock(configMutex_);
    if (configState_ == ConfigState::STOPPING) {
        const std::string reason = "Coordinator UpdateConfig: runtime is stopping";
        OperationLogger::Instance().LogConfigApiFailed("UpdateConfig", reason);
        return Status(K_SHUTTING_DOWN, reason);
    }
    if (configState_ != ConfigState::READY || configUpdater_ == nullptr) {
        const std::string reason = "Coordinator UpdateConfig: runtime is not ready";
        OperationLogger::Instance().LogConfigApiFailed("UpdateConfig", reason);
        return Status(K_NOT_READY, reason);
    }
    return configUpdater_->ApplyJson(configJson, "Coordinator UpdateConfig");
}

Status CoordinatorRuntime::Stop()
{
    {
        std::lock_guard<std::mutex> lock(configMutex_);
        configState_ = ConfigState::STOPPING;
    }
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

void CoordinatorRuntime::EnableConfigUpdates()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (!stopRequested_) {
        std::lock_guard<std::mutex> configLock(configMutex_);
        if (configState_ == ConfigState::NOT_READY) {
            configState_ = ConfigState::READY;
        }
    }
}

void CoordinatorRuntime::DisableConfigUpdates()
{
    std::lock_guard<std::mutex> lock(configMutex_);
    configState_ = ConfigState::STOPPING;
}

void CoordinatorRuntime::RunEventLoop()
{
    std::unique_lock<std::mutex> lock(mutex_);
    while (!stopRequested_ && !IsTermSignalReceived()) {
        stopCv_.wait_for(lock, std::chrono::milliseconds(kStopPollIntervalMs),
                         [this] { return stopRequested_ || IsTermSignalReceived(); });
        if (stopRequested_ || IsTermSignalReceived()) {
            break;
        }
        lock.unlock();
        metrics::Tick();
        lock.lock();
    }
    DisableConfigUpdates();
    lock.unlock();
    LOG(INFO) << "Coordinator Runtime stop requested, shutting down";
}

}  // namespace datasystem
