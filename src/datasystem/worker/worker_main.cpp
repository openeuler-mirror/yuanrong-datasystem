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

#include <fstream>
#include <string>
#include <libgen.h>
#include <sys/prctl.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
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

int main(int argc, char **argv)
{
    Flags flags;
    auto rc = Worker::GetInstance()->Init(flags, argc, argv);
    if (rc.IsError()) {
        LOG(ERROR) << "Worker runtime error:" << rc.ToString();
        LOG_IF_ERROR(Worker::GetInstance()->ShutDown(), "worker shutdown failed");
        return -1;
    }
    PerfManager *perfManager = PerfManager::Instance();

    Timer timer;
    {
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
            // Check whether the configuration file is updated every 10 seconds.
            flags.MonitorConfigFile(FLAGS_monitor_config_file);
        }
    }
    if (perfManager != nullptr) {
        perfManager->PrintPerfLog();
    }
    rc = Worker::GetInstance()->ShutDown();
    if (rc.IsError()) {
        LOG(ERROR) << "Worker runtime error:" << rc.ToString();
        return -1;
    }
    return 0;
}
