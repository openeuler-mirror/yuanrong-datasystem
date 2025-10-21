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
 * Description: ZMQ perf agent main
 */
#include <signal.h>
#include <unistd.h>

#include "zmq_perf_agent.h"

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"

DS_DECLARE_string(log_filename);
volatile sig_atomic_t g_exitFlag = 0;
void SignalHandler(int signum)
{
    (void)signum;
    g_exitFlag = 1;
}
/**
 * This is the client program to run zmq benchmark.
 * Please make sure the server program is up and running.
 * @param argc
 * @param argv
 * @return
 */
int main(int argc, char **argv)
{
    auto pid = getpid();
    FLAGS_log_filename = datasystem::FormatString("%s.%d", "zmq_perf_agent", pid);
    FLAGS_v = datasystem::RPC_KEY_LOG_LEVEL;
    datasystem::Logging::GetInstance()->Start(FLAGS_log_filename);
    datasystem::st::ZmqPerfAgent zmqPerfAgent;
    auto rc = zmqPerfAgent.ProcessArgs(argc, argv);
    if (rc == 0) {
        std::cout << datasystem::FormatString("Perf agent spawns successfully (pid %d)\n", pid);
        auto status = zmqPerfAgent.Run();
        if (status.IsError()) {
            std::cerr << status.ToString() << std::endl;
            return status.GetCode();
        }
        while (!g_exitFlag) {
            usleep(100000);
        }
    }
    return 0;
}
