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
#include "zmq_perf_run.h"

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/rpc/zmq/zmq_constants.h"

DS_DECLARE_string(log_filename);
/**
 * This is the client program to run zmq benchmark.
 * Please make sure the server program is up and running.
 * @param argc
 * @param argv
 * @return
 */
int main(int argc, char **argv)
{
    FLAGS_log_filename = "zmq_perf_client";
    FLAGS_v = datasystem::RPC_KEY_LOG_LEVEL;
    datasystem::Logging::GetInstance()->Start(FLAGS_log_filename);
    datasystem::st::ZmqPerfRun zmqPerfRun;
    auto rc = zmqPerfRun.ProcessArgs(argc, argv);
    if (rc == 0) {
        std::cout << "Run set up:\n" << zmqPerfRun << std::endl;
        auto status = zmqPerfRun.RunAllBenchmarks();
        if (status.IsError()) {
            std::cerr << status.ToString() << std::endl;
            return status.GetCode();
        }
    }
    return 0;
}
