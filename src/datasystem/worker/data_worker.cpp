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

/**
 * Description: Worker lifecycle management — startup, event loop, and shutdown.
 */
#include "datasystem/data_worker.h"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <cerrno>
#include <cstring>
#include <sys/prctl.h>

#include "datasystem/common/encrypt/secret_manager.h"
#include "datasystem/common/flags/flag_manager.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/kvstore/rocksdb/rocks_store.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/log/operation_logger.h"
#include "datasystem/common/log/failure_handler.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/metrics/metrics.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/signal/signal.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/flags/dynamic_flag_config.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/common/util/version.h"
#include "datasystem/worker/worker_cli.h"
#include "datasystem/worker/worker_oc_server.h"
#include "datasystem/worker/worker_sched_runtime.h"
#include "datasystem/worker/worker_service_accessor.h"
#include "datasystem/worker/worker_update_flag_check.h"
// worker_oc_server.h pulls in brpc headers which override LOG/VLOG/DLOG.
// Re-include log.h to restore datasystem's spdlog-based macros
// (the restore block lives outside log.h's include guard).
#include "datasystem/common/log/log.h"

DS_DECLARE_string(worker_address);
DS_DECLARE_string(bind_address);
DS_DECLARE_string(master_address);
DS_DECLARE_uint64(shared_memory_size_mb);
DS_DECLARE_bool(enable_curve_zmq);
DS_DECLARE_string(log_filename);
DS_DECLARE_string(monitor_config_file);
DS_DECLARE_bool(enable_thp);
DS_DECLARE_string(etcd_address);
DS_DECLARE_string(etcd_ca);
DS_DECLARE_string(etcd_cert);
DS_DECLARE_string(etcd_key);
DS_DECLARE_string(rocksdb_store_dir);
DS_DECLARE_string(log_dir);
DS_DECLARE_uint32(max_log_size);
DS_DECLARE_int32(logfile_mode);

#ifdef WITH_TESTS
DS_DEFINE_string_dynamic(inject_actions, "", "Set inject action when worker start for ut.");
#endif

extern "C" {
void *CreateWorker()
{
    return static_cast<void *>(datasystem::DataWorker::GetInstance());
}

void WorkerDestroy(void *w)
{
    auto *worker = static_cast<datasystem::DataWorker *>(w);
    LOG_IF_ERROR(worker->StopEmbeddedWorker(), "worker stop embedded failed");
}

Status InitEmbeddedWorker(const EmbeddedConfig &config, void *w)
{
    return static_cast<datasystem::DataWorker *>(w)->InitEmbeddedWorker(config);
}
}

#define SHUTDOWN_IF_NOT_OK(statement_)    \
    do {                                  \
        Status rc_ = (statement_);        \
        if (rc_.IsError()) {              \
            LOG(ERROR) << rc_.ToString(); \
            (void)worker_->Shutdown();    \
            return rc_;                   \
        }                                 \
    } while (false)

namespace datasystem {
std::condition_variable g_termSignalCv;

void SignalHandler(int signum)
{
    (void)signum;
    g_exitFlag = 1;
    g_termSignalCv.notify_all();
}
struct WorkerServerOptions {
    HostPort workerAddress;
    HostPort bindAddress;
    HostPort masterAddress;
    Status LoadParameters()
    {
        CHECK_FAIL_RETURN_STATUS(!FLAGS_worker_address.empty(), StatusCode::K_RUNTIME_ERROR,
                                 "Cannot get worker address.");
        Status s = workerAddress.ParseString(FLAGS_worker_address);
        if (!s.IsOk()) {
            RETURN_STATUS(StatusCode::K_INVALID, "Worker address is invalid");
        }

        if (FLAGS_bind_address.empty()) {
            FLAGS_bind_address = FLAGS_worker_address;
        }

        s = bindAddress.ParseString(FLAGS_bind_address);
        if (!s.IsOk()) {
            RETURN_STATUS(StatusCode::K_INVALID, "Bind address is invalid");
        }
        if (!FLAGS_master_address.empty()) {
            s = masterAddress.ParseString(FLAGS_master_address);
            if (!s.IsOk()) {
                RETURN_STATUS(StatusCode::K_INVALID, "Master address is invalid");
            }
        }
        LOG(INFO) << "GOT MASTER ADDRESS: " << masterAddress.ToString() << " at worker: " << workerAddress.ToString()
                  << " bind on: " << bindAddress.ToString();
        return Status::OK();
    }
};

constexpr int CHECK_EVERY_MS = 1'000;
constexpr int MAX_TOLERATED_ATTEMPTS = 10;
constexpr int REPORTING_THRESHOLD_MS = CHECK_EVERY_MS * MAX_TOLERATED_ATTEMPTS;

/*
 * Keep all post processing may cause early return in WorkerPostProcessing() function
 * so we can ensure worker.Shutdown() run before WorkerMain() return.
 * Don't use raii which is hard to control the order of destructors being called.
 */
Status WorkerPostProcessing()
{
    if (SecretManager::Instance()->IsRootKeyActive()) {
        // Destroy key component and rootkey.
#ifndef WITH_TESTS
        RETURN_IF_NOT_OK(SecretManager::Instance()->DestroyRootKey());
#endif
    }
    return Status::OK();
}

Status DisableTHP()
{
    if (FLAGS_enable_thp) {
        return Status::OK();
    }
    auto ret = prctl(PR_SET_THP_DISABLE, 1, 0, 0, 0);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(ret == 0, K_RUNTIME_ERROR,
                                         FormatString("Failed to disable THP, and the errno is : %d", errno));
    return Status::OK();
}

/**
 * @brief After the rocksdb version is upgraded, it need to initialize the rocksdb database in advance to avoid abort.
 */
Status PreInitRocksDB()
{
    RETURN_IF_NOT_OK(Uri::NormalizePathWithUserHomeDir(FLAGS_rocksdb_store_dir, "~/.datasystem/rocksdb", "/master"));
    std::string preInitRocksDir = FLAGS_rocksdb_store_dir + "/pre-start";
    RETURN_IF_NOT_OK(RemoveAll(preInitRocksDir));
    if (!FileExist(preInitRocksDir)) {
        // The permission of ~/.datasystem/rocksdb/object_metadata.
        const int permission = 0700;
        RETURN_IF_NOT_OK(CreateDir(preInitRocksDir, true, permission));
    }
    std::shared_ptr<RocksStore> workerRocks = RocksStore::GetInstance(preInitRocksDir);
    CHECK_FAIL_RETURN_STATUS(workerRocks != nullptr, StatusCode::K_RUNTIME_ERROR, "Rocksdb has been initialized.");
    workerRocks->Close();
    return Status::OK();
}

void TrySetInjectActions()
{
#ifdef WITH_TESTS
    LOG_IF_ERROR(inject::SetByString(FLAGS_inject_actions), "set inject actions failed");
#endif
}

void InitWorkerLogConfig()
{
    std::string errMsg;
    if (FLAGS_log_dir.empty() || !Validator::ValidatePathString("log_dir", FLAGS_log_dir)) {
        SetCommandLineOption("log_dir", DEFAULT_LOG_DIR, errMsg);
    }

    if (!Validator::ValidateUint32("max_log_size", FLAGS_max_log_size)) {
        auto val = std::to_string(DEFAULT_MAX_LOG_SIZE_MB);
        SetCommandLineOption("max_log_size", val, errMsg);
    }
}

constexpr size_t ERR_MSG_BUF_SIZE = 256;

std::string SchedStrError(int err)
{
    char buf[ERR_MSG_BUF_SIZE] = { 0 };
#if defined(__GLIBC__) && defined(_GNU_SOURCE)
    return std::string(strerror_r(err, buf, sizeof(buf)));
#else
    auto ret = strerror_r(err, buf, sizeof(buf));
    if (ret != 0) {
        return FormatString("Unknown error %d", err);
    }
    return std::string(buf);
#endif
}

void LogSetWorkerSchedRuntimeResult(const SetSchedRuntimeResult &result)
{
    if (!result.success) {
        LOG(WARNING) << FormatString("Failed to set worker sched runtime to %llu ns, errno: %d, error: %s",
            static_cast<unsigned long long>(GetWorkerSchedRuntimeNs()), result.err, SchedStrError(result.err));
        return;
    }
    LOG(INFO) << "Set worker sched runtime to " << GetWorkerSchedRuntimeNs() << " ns.";
}

DataWorker *DataWorker::GetInstance()
{
    static DataWorker instance;
    return &instance;
}

Status DataWorker::ShutDown()
{
    worker::WorkerServiceAccessor::Instance().Unregister();
    if (worker_) {
        RETURN_IF_NOT_OK(worker_->Shutdown());
        worker_.reset();
    }
    return Status::OK();
}

Status DataWorker::PreShutDown()
{
    if (worker_) {
        RETURN_IF_NOT_OK(worker_->PreShutDown());
    }
    return Status::OK();
}

DataWorker::~DataWorker()
{
    LOG_IF_ERROR(PreShutDown(), "worker pre-shutdown failed");
    LOG_IF_ERROR(ShutDown(), "worker shutdown failed");
}

Status DataWorker::Stop()
{
    LOG(INFO) << "DataWorker::Stop() called, requesting shutdown";
    g_exitFlag = 1;
    g_termSignalCv.notify_all();
    return Status::OK();
}

Status DataWorker::StopEmbeddedWorker()
{
    LOG(INFO) << "DataWorker::StopEmbeddedWorker() called";
    LOG_IF_ERROR(PreShutDown(), "worker pre-shutdown failed");
    LOG_IF_ERROR(ShutDown(), "worker shut down failed");
    return Status::OK();
}

Status DataWorker::UpdateConfig(const std::string &configJson)
{
    if (!worker_) {
        return Status(StatusCode::K_RUNTIME_ERROR, "UpdateConfig: worker not initialized");
    }
    return worker_->UpdateConfig(configJson);
}

Status DataWorker::InitWorker(DynamicFlagConfig &flags, const bool isEmbeddedClient)
{
    InitWorkerLogConfig();
    RETURN_IF_NOT_OK(Uri::NormalizePathWithUserHomeDir(FLAGS_log_dir, DEFAULT_LOG_DIR, "/worker"));

    Logging::GetInstance()->Start(FLAGS_log_filename, false, LOG_ROLLING_COMPRESS_SECS, isEmbeddedClient);

    LOG(INFO) << "Git Commit:" << GIT_HASH << "; Git Branch: " << GIT_BRANCH;

#ifdef SUPPORT_JEPROF
    LOG(WARNING) << "Worker support jeprof.";
#endif

    TrySetInjectActions();
    if (!isEmbeddedClient) {
        // We need to handle the SIGPIPE or worker will die when
        // we send message to a disconnected client via UDS.
        (void)signal(SIGPIPE, SIG_IGN);
        (void)signal(SIGINT, SignalHandler);
        (void)signal(SIGTERM, SignalHandler);
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(LoadAllSecretKeys(), "Load secret keys failed.");

    LOG_IF_ERROR(DisableTHP(), "Control the thp is failed");

    WorkerServerOptions options;
    RETURN_IF_NOT_OK(options.LoadParameters());
    worker_ = std::make_unique<worker::WorkerOCServer>(
        options.workerAddress, options.bindAddress, options.masterAddress);
    worker_->SetFlags(&flags);

    LOG_IF_ERROR(PreInitRocksDB(), "Failed to initialize the rocksdb database in advance.");

    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    SHUTDOWN_IF_NOT_OK(worker_->Init());
    SHUTDOWN_IF_NOT_OK(worker_->Start());
    SHUTDOWN_IF_NOT_OK(WorkerPostProcessing());

    LOG_IF_ERROR(Uri::NormalizePathWithUserHomeDir(FLAGS_monitor_config_file, "", ""),
                 FormatString("Failed to normalize the path (%s) with user home directory", FLAGS_monitor_config_file));
    LOG(INFO) << "Worker start success";
    OperationLogger::Instance().LogConfigInit(flags.GetAllFlagsStr());

    return Status::OK();
}

Status DataWorker::InitEmbeddedWorker(const EmbeddedConfig &config)
{
    std::lock_guard<std::mutex> lock(initMutex_);
    if (started_.load()) {
        return Status(StatusCode::K_DUPLICATED, "Worker already started");
    }

    SetVersionString(DATASYSTEM_VERSION);
    SetUsageMessage(
        "Provide POSIX data semantic service interfaces (data objects (KVs) and stream). "
        "It builds the local cache capability based on the proximity computing memory space "
        "to cache hotspot data.");

    DynamicFlagConfig flags;
    flags.SetValidateSpecial(WorkerFlagValidateSpecial);
    FlagInfoMap defaultGflagMap = flags.GetAllFlagsToMap();
    FLAGS_log_filename = "datasystem_worker";
    FLAGS_logfile_mode = 0640;  // 0640: default permission for log files.
    std::string errMsg;
    CHECK_FAIL_RETURN_STATUS(ParseCommandLineFlags(config, errMsg), K_INVALID,
                             FormatString("parse worker flags failed, err: %s", errMsg));
    if (cli::HandleCli()) {
        return Status::OK();
    }
    // The configuration file processing is triggered immediately when the worker is started.
    flags.StartConfigFileHandle(FLAGS_monitor_config_file, std::chrono::steady_clock::now());
    AdjustNodeTimeoutFlags();
    CHECK_FAIL_RETURN_STATUS(ValidateWatermarkFlags(), K_INVALID, "invalid evict/spill watermark flag configuration");
    auto rc = InitWorker(flags, true);
    if (rc.IsOk()) {
        worker::WorkerServiceAccessor::Instance().Register(worker_.get());
        started_.store(true);
    }
    LOG(INFO) << "Worker non-default flags:\n" << flags.GetNonDefaultFlags(defaultGflagMap);
    return rc;
}

/// @brief Run the event loop, then perform PreShutDown and ShutDown.
/// @details Blocks until a termination signal or Stop() is received.
///          Shared by both InitAndRun overloads.
void DataWorker::RunEventLoopAndShutdown(DynamicFlagConfig &flags)
{
    Trace::Instance().SetTraceNewID("WorkerMain;" + GetStringUuid(), true);
    PerfManager *perfManager = PerfManager::Instance();
    Timer timer;
    std::unique_lock<std::mutex> termSignalLock(g_termSignalMutex);
    while (!IsTermSignalReceived()) {
        bool signalReceived = g_termSignalCv.wait_for(termSignalLock, std::chrono::milliseconds(CHECK_EVERY_MS),
                                                      [] { return IsTermSignalReceived(); });
        if (signalReceived) {
            break;
        }
        auto elapsedMs = timer.ElapsedMilliSecondAndReset();
        if (elapsedMs > REPORTING_THRESHOLD_MS) {
            LOG(ERROR) << FormatString("Worker was hanged about %.2f ms", elapsedMs);
        }
        if (perfManager != nullptr) {
            perfManager->Tick();
        }
        metrics::Tick();
        if (!FLAGS_monitor_config_file.empty()) {
            flags.MonitorConfigFile(FLAGS_monitor_config_file);
        }
    }
    termSignalLock.unlock();

    if (perfManager != nullptr) {
        perfManager->PrintPerfLog();
    }
    metrics::PrintSummary();

    LOG_IF_ERROR(PreShutDown(), "worker pre-shutdown failed");
    LOG_IF_ERROR(ShutDown(), "worker shutdown failed");
}

Status DataWorker::DoInit(DynamicFlagConfig &flags, const char *crashReporterLabel)
{
    auto setSchedRuntimeResult = SetWorkerSchedRuntime();
    auto rc = InitWorker(flags, false);
    if (rc.IsError()) {
        LOG(ERROR) << "Worker runtime error:" << rc.ToString();
        LogSetWorkerSchedRuntimeResult(setSchedRuntimeResult);
        LOG_IF_ERROR(ShutDown(), "worker shutdown failed");
        return rc;
    }
    if (!IsTermSignalReceived()) {
        LogSetWorkerSchedRuntimeResult(setSchedRuntimeResult);
    }

    worker::WorkerServiceAccessor::Instance().Register(worker_.get());
    InstallFailureSignalHandler(crashReporterLabel);
    started_.store(true);
    return Status::OK();
}

Status DataWorker::InitAndRun(int argc, char **argv)
{
    DynamicFlagConfig flags;
    {
        std::lock_guard<std::mutex> lock(initMutex_);
        if (started_.load()) {
            return Status(StatusCode::K_DUPLICATED, "Worker already started");
        }

        SetVersionString(DATASYSTEM_VERSION);
        SetUsageMessage(
            "Provide POSIX data semantic service interfaces (data objects (KVs) and stream). "
            "It builds the local cache capability based on the proximity computing memory space "
            "to cache hotspot data.");

        flags.SetValidateSpecial(WorkerFlagValidateSpecial);
        FlagInfoMap defaultGflagMap = flags.GetAllFlagsToMap();
        FLAGS_logfile_mode = 0640;
        LinkCommonFlagsValidators();
        ParseCommandLineFlags(argc, argv);
        if (cli::HandleCli()) {
            SignalHandler(0);
            return Status::OK();
        }
        flags.StartConfigFileHandle(FLAGS_monitor_config_file, std::chrono::steady_clock::now());
        AdjustNodeTimeoutFlags();
        CHECK_FAIL_RETURN_STATUS(ValidateWatermarkFlags(), K_INVALID,
                                 "invalid evict/spill watermark flag configuration");
        RETURN_IF_NOT_OK(flags.EraseInfo(argc, argv));
        RETURN_IF_NOT_OK(DoInit(flags, argv[0]));
        LOG(INFO) << "Worker non-default flags:\n" << flags.GetNonDefaultFlags(defaultGflagMap);
    }  // initMutex_ released here; subsequent threads will see started_==true

    RunEventLoopAndShutdown(flags);
    return Status::OK();
}

Status DataWorker::InitAndRun(const DataWorkerOptions &options)
{
    DynamicFlagConfig flags;
    {
        std::lock_guard<std::mutex> lock(initMutex_);
        if (started_.load()) {
            return Status(StatusCode::K_DUPLICATED, "Worker already started");
        }

        SetVersionString(DATASYSTEM_VERSION);
        SetUsageMessage(
            "Provide POSIX data semantic service interfaces (data objects (KVs) and stream). "
            "It builds the local cache capability based on the proximity computing memory space "
            "to cache hotspot data.");

        flags.SetValidateSpecial(WorkerFlagValidateSpecial);
        FlagInfoMap defaultGflagMap = flags.GetAllFlagsToMap();
        FLAGS_logfile_mode = 0640;

        LinkCommonFlagsValidators();
        std::string errMsg;
        CHECK_FAIL_RETURN_STATUS(
            FlagManager::GetInstance()->ParseConfigFile(options.configFilePath, errMsg),
            K_INVALID, FormatString("Parse config file %s error: %s", options.configFilePath, errMsg));

        flags.StartConfigFileHandle(FLAGS_monitor_config_file, std::chrono::steady_clock::now());
        AdjustNodeTimeoutFlags();
        CHECK_FAIL_RETURN_STATUS(ValidateWatermarkFlags(), K_INVALID,
                                 "invalid evict/spill watermark flag configuration");
        RETURN_IF_NOT_OK(DoInit(flags, "datasystem_worker"));
        LOG(INFO) << "Worker non-default flags:\n" << flags.GetNonDefaultFlags(defaultGflagMap);
    }  // initMutex_ released here; subsequent threads will see started_==true

    RunEventLoopAndShutdown(flags);
    return Status::OK();
}

}  // namespace datasystem
