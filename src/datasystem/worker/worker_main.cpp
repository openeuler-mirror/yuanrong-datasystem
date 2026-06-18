/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Worker program master file.
 */

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <string>
#include <libgen.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/metrics/metrics.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/signal/signal.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/gflag/flags.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/common/util/version.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/worker/worker.h"
#include "datasystem/worker/worker_cli.h"
#include "datasystem/worker/worker_oc_server.h"
#include "datasystem/worker/worker_update_flag_check.h"

DS_DECLARE_string(monitor_config_file);

using namespace datasystem;
using namespace datasystem::worker;
static constexpr int CHECK_EVERY_MS = 1'000;
static constexpr int MAX_TOLERATED_ATTEMPTS = 10;
static constexpr int REPORTING_THRESHOLD_MS = CHECK_EVERY_MS * MAX_TOLERATED_ATTEMPTS;
static constexpr size_t ERR_MSG_BUF_SIZE = 256;

struct SetSchedRuntimeResult {
    bool success;
    int err;
};

SetSchedRuntimeResult SetWorkerSchedRuntime();
uint64_t GetWorkerSchedRuntimeNs();

std::string StrError(int err)
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
                                     static_cast<unsigned long long>(GetWorkerSchedRuntimeNs()), result.err,
                                     StrError(result.err));
        return;
    }
    LOG(INFO) << "Set worker sched runtime to " << GetWorkerSchedRuntimeNs() << " ns.";
}

int InitWorkerOrExit(Flags &flags, int argc, char **argv, const SetSchedRuntimeResult &setSchedRuntimeResult)
{
    auto rc = Worker::GetInstance()->Init(flags, argc, argv);
    if (rc.IsError()) {
        LOG(ERROR) << "Worker runtime error:" << rc.ToString();
        LogSetWorkerSchedRuntimeResult(setSchedRuntimeResult);
        LOG_IF_ERROR(Worker::GetInstance()->ShutDown(), "worker shutdown failed");
        return -1;
    }
    if (!IsTermSignalReceived()) {
        LogSetWorkerSchedRuntimeResult(setSchedRuntimeResult);
    }
    return 0;
}

void RunWorkerUntilSignal(Flags &flags, PerfManager *perfManager)
{
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
}

int ShutdownWorkerOrExit()
{
    auto rc = Worker::GetInstance()->PreShutDown();
    if (rc.IsError()) {
        LOG(ERROR) << "Worker preShutDown error:" << rc.ToString();
        LOG_IF_ERROR(Worker::GetInstance()->ShutDown(), "worker preshutdown failed");
        return -1;
    }
    rc = Worker::GetInstance()->ShutDown();
    if (rc.IsError()) {
        LOG(ERROR) << "Worker runtime error:" << rc.ToString();
        return -1;
    }
    return 0;
}

int main(int argc, char **argv)
{
    auto setSchedRuntimeResult = SetWorkerSchedRuntime();
    Flags flags;
    int initRc = InitWorkerOrExit(flags, argc, argv, setSchedRuntimeResult);
    if (initRc != 0) {
        return initRc;
    }
    PerfManager *perfManager = PerfManager::Instance();
    RunWorkerUntilSignal(flags, perfManager);
    if (perfManager != nullptr) {
        perfManager->PrintPerfLog();
    }
    metrics::PrintSummary();
    return ShutdownWorkerOrExit();
}
