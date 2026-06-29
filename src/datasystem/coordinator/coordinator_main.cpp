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
 * Description: Lightweight coordinator server entrypoint.
 */
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <mutex>

#include "datasystem/common/log/log.h"
#include "datasystem/common/signal/signal.h"
#include "datasystem/common/util/gflag/flags.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/coordinator/coordinator_server.h"

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

void WaitForTermSignal()
{
    std::unique_lock<std::mutex> termSignalLock(g_termSignalMutex);
    while (!IsTermSignalReceived()) {
        g_termSignalCv.wait_for(termSignalLock, std::chrono::milliseconds(CHECK_EVERY_MS),
                                [] { return IsTermSignalReceived(); });
    }
}

Status RunCoordinator()
{
    coordinator::CoordinatorServer server;
    RETURN_IF_NOT_OK(server.Init());
    RETURN_IF_NOT_OK(server.Start());
    WaitForTermSignal();
    return server.Shutdown();
}
}  // namespace
}  // namespace datasystem

int main(int argc, char **argv)
{
    signal(SIGTERM, datasystem::SignalHandler);
    signal(SIGINT, datasystem::SignalHandler);
    datasystem::ParseCommandLineFlags(argc, argv);

    auto rc = datasystem::RunCoordinator();
    if (rc.IsError()) {
        LOG(ERROR) << "Coordinator runtime error: " << rc.ToString();
        return -1;
    }
    return 0;
}
