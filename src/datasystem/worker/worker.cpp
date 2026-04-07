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
 * Description: Worker for start and shutdown worker.
 */
#include "datasystem/worker/worker.h"

#include <fstream>
#include <string>
#include <libgen.h>
#include <sys/prctl.h>

#include "datasystem/common/encrypt/secret_manager.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/kvstore/rocksdb/rocks_store.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/log/failure_handler.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/signal/signal.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/gflag/flags.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/worker/worker_cli.h"
#include "datasystem/worker/worker_oc_server.h"
#include "datasystem/worker/worker_update_flag_check.h"

DS_DECLARE_string(worker_address);
DS_DECLARE_string(bind_address);
DS_DECLARE_string(master_address);
DS_DECLARE_uint64(shared_memory_size_mb);
DS_DECLARE_bool(enable_curve_zmq);
DS_DECLARE_string(log_filename);
DS_DECLARE_string(monitor_config_file);
DS_DEFINE_bool(
    enable_thp, false,
    "Control this process by enabling transparent huge pages, default is disabled. Enable Transparent Huge Pages (THP) "
    "can enhance performance and reduce page table overhead, but it may also lead to increased memory usage");
DS_DECLARE_string(etcd_address);
DS_DECLARE_string(etcd_ca);
DS_DECLARE_string(etcd_cert);
DS_DECLARE_string(etcd_key);
DS_DECLARE_string(rocksdb_store_dir);
DS_DECLARE_string(log_dir);
DS_DECLARE_uint32(max_log_size);
DS_DECLARE_int32(logfile_mode);

#ifdef WITH_TESTS
DS_DEFINE_string(inject_actions, "", "Set inject action when worker start for ut.");
#endif

extern "C" {
void *CreateWorker()
{
    return static_cast<void *>(datasystem::worker::Worker::GetInstance());
}

void WorkerDestroy(void *w)
{
    auto status = static_cast<datasystem::worker::Worker *>(w)->ShutDown();
    LOG_IF_ERROR(status, "worker shut down failed");
}

Status InitEmbeddedWorker(const EmbeddedConfig &config, void *w)
{
    return static_cast<datasystem::worker::Worker *>(w)->InitEmbeddedWorker(config);
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
namespace worker {
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

Worker *Worker::GetInstance()
{
    static Worker instance;
    return &instance;
}

Status Worker::ShutDown()
{
    if (worker_) {
        SHUTDOWN_IF_NOT_OK(worker_->PreShutDown());
        RETURN_IF_NOT_OK(worker_->Shutdown());
        worker_.reset();
    }
    return Status::OK();
}

Worker::~Worker()
{
    LOG_IF_ERROR(ShutDown(), "worker shutdown failed");
}

WorkerServiceImpl *Worker::GetWorkerService()
{
    return worker_->GetWorkerService();
}

object_cache::WorkerOCServiceImpl *Worker::GetWorkerOCService()
{
    return worker_->GetWorkerOCService();
}

Status Worker::InitWorker(Flags &flags, const GFlagsMap &defaultGflagMap, const bool isEmbeddedClient)
{
    InitWorkerLogConfig();
    RETURN_IF_NOT_OK(Uri::NormalizePathWithUserHomeDir(FLAGS_log_dir, DEFAULT_LOG_DIR, "/worker"));

    Logging::GetInstance()->Start(FLAGS_log_filename, false, LOG_ROLLING_COMPRESS_SECS, isEmbeddedClient);

#ifdef SUPPORT_JEPROF
    LOG(WARNING) << "Worker support jeprof.";
#endif

    LOG(INFO) << "Git Commit:" << GIT_HASH << "; Git Branch: " << GIT_BRANCH;
    LOG(INFO) << "Worker non-default flags:\n" << flags.GetNonDefaultFlags(defaultGflagMap);
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
    worker_ = std::make_unique<WorkerOCServer>(options.workerAddress, options.bindAddress, options.masterAddress);

    LOG_IF_ERROR(PreInitRocksDB(), "Failed to initialize the rocksdb database in advance.");

    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    SHUTDOWN_IF_NOT_OK(worker_->Init());
    SHUTDOWN_IF_NOT_OK(worker_->Start());
    SHUTDOWN_IF_NOT_OK(WorkerPostProcessing());

    LOG_IF_ERROR(Uri::NormalizePathWithUserHomeDir(FLAGS_monitor_config_file, "", ""),
                 FormatString("Failed to normalize the path (%s) with user home directory", FLAGS_monitor_config_file));
    LOG(INFO) << "Worker start success";

    return Status::OK();
}

Status Worker::InitEmbeddedWorker(const EmbeddedConfig &config)
{
    SetVersionString(DATASYSTEM_VERSION);
    SetUsageMessage(
        "Provide POSIX data semantic service interfaces (data objects (KVs) and stream). "
        "It builds the local cache capability based on the proximity computing memory space to cache hotspot data.");
    Flags flags;
    flags.SetValidateSpecial(WorkerFlagValidateSpecial);
    GFlagsMap defaultGflagMap = flags.GetAllFlagsToMap();
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
    return InitWorker(flags, defaultGflagMap, true);
}

Status Worker::Init(Flags &flags, int argc, char **argv)
{
    SetVersionString(DATASYSTEM_VERSION);
    SetUsageMessage(
        "Provide POSIX data semantic service interfaces (data objects (KVs) and stream). "
        "It builds the local cache capability based on the proximity computing memory space to cache hotspot data.");

    flags.SetValidateSpecial(WorkerFlagValidateSpecial);
    GFlagsMap defaultGflagMap = flags.GetAllFlagsToMap();
    FLAGS_logfile_mode = 0640;  // 0640: default permission for log files.
    ParseCommandLineFlags(argc, argv);
    if (cli::HandleCli()) {
        return Status::OK();
    }
    // The configuration file processing is triggered immediately when the worker is started.
    flags.StartConfigFileHandle(FLAGS_monitor_config_file, std::chrono::steady_clock::now());
    RETURN_IF_NOT_OK(flags.EraseInfo(argc, argv));

    InstallFailureSignalHandler(argv[0]);
    return InitWorker(flags, defaultGflagMap, false);
    ;
}
}  // namespace worker
}  // namespace datasystem
