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
 * Description: ZMQ perf client main
 */
#include <signal.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/flags/flags.h"
#include "zmq_perf_server.h"

volatile sig_atomic_t g_exitFlag = 0;
void SignalHandler(int signum)
{
    (void)signum;
    g_exitFlag = 1;
}
DS_DECLARE_string(log_filename);
/**
 * This is the ZMQ server for benchmark run.
 * @param argc
 * @param argv
 * @return
 */
int main(int argc, char **argv)
{
    FLAGS_log_filename = "zmq_perf_server";
    FLAGS_v = datasystem::RPC_KEY_LOG_LEVEL;
    datasystem::Logging::GetInstance()->Start(FLAGS_log_filename);
    signal(SIGINT, SignalHandler);
    signal(SIGTERM, SignalHandler);
    datasystem::st::ZmqPerfServer zmqPerfServer;
    auto rc = zmqPerfServer.ProcessArgs(argc, argv);
    if (rc == 0) {
        auto status = zmqPerfServer.Run();
        if (status.IsOk()) {
            std::cout << "Server is up and running.\n";
            std::cout << zmqPerfServer;
            std::cout << "Hit Control-C to stop the server" << std::endl;
            // Loop forever until shutdown
            while (!g_exitFlag) {
                usleep(100000);
            }
        } else {
            std::cerr << status.ToString() << std::endl;
            return status.GetCode();
        }
    }
    return 0;
}
